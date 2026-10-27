/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/functions/sparksql/VariantJson.h"

#include <cmath>
#include <ctime>

#include <folly/Conv.h>
#include <folly/json.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/encode/Base64.h"
#include "velox/type/Conversions.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"

namespace facebook::velox::functions::sparksql {
namespace {

// Limits of the Jackson parser Spark uses.
constexpr int32_t kMaxNestingDepth{1'000};
constexpr size_t kMaxNumberLength{1'000};

constexpr int32_t kMaxDecimalPrecision{38};

// Reads JSON text into a VariantBuilder.
class JsonReader {
 public:
  JsonReader(std::string_view json, VariantBuilder& builder)
      : json_{json}, builder_{builder} {}

  void read() {
    skipWhitespace();
    if (atEnd()) {
      fail("Empty input.");
    }
    readValue(0, /*isRoot=*/true);
  }

 private:
  [[noreturn]] void fail(std::string_view reason) const {
    VELOX_USER_FAIL(
        "Malformed records are detected in record parsing: {} at position {}: {}",
        reason,
        pos_,
        json_);
  }

  bool atEnd() const {
    return pos_ >= json_.size();
  }

  char peek() const {
    return json_[pos_];
  }

  void skipWhitespace() {
    while (!atEnd()) {
      const char c = peek();
      if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
        return;
      }
      ++pos_;
    }
  }

  void expect(char expected) {
    skipWhitespace();
    if (atEnd() || peek() != expected) {
      fail(fmt::format("Expected '{}'", expected));
    }
    ++pos_;
  }

  void readValue(int32_t depth, bool isRoot) {
    if (atEnd()) {
      fail("Unexpected end of input");
    }
    switch (peek()) {
      case '{':
        readObject(depth + 1);
        break;
      case '[':
        readArray(depth + 1);
        break;
      case '"': {
        std::string value;
        readString(value);
        builder_.appendString(value);
        break;
      }
      case 't':
        readLiteral("true");
        builder_.appendBoolean(true);
        break;
      case 'f':
        readLiteral("false");
        builder_.appendBoolean(false);
        break;
      case 'n':
        readLiteral("null");
        builder_.appendNull();
        break;
      default:
        readNumber(isRoot);
        break;
    }
  }

  void checkDepth(int32_t depth) const {
    if (depth > kMaxNestingDepth) {
      fail("Nesting depth exceeds the maximum");
    }
  }

  void readObject(int32_t depth) {
    checkDepth(depth);
    ++pos_;
    const auto start = builder_.writePosition();
    std::vector<VariantBuilder::Field> fields;
    skipWhitespace();
    if (!atEnd() && peek() == '}') {
      ++pos_;
      builder_.finishObject(start, fields);
      return;
    }
    std::string key;
    while (true) {
      skipWhitespace();
      if (atEnd() || peek() != '"') {
        fail("Expected a field name");
      }
      key.clear();
      readString(key);
      expect(':');
      skipWhitespace();
      fields.push_back(
          {builder_.addKey(key), builder_.writePosition() - start});
      readValue(depth, /*isRoot=*/false);
      skipWhitespace();
      if (atEnd()) {
        fail("Unexpected end of input");
      }
      if (peek() == ',') {
        ++pos_;
        continue;
      }
      if (peek() == '}') {
        ++pos_;
        break;
      }
      fail("Expected ',' or '}'");
    }
    builder_.finishObject(start, fields);
  }

  void readArray(int32_t depth) {
    checkDepth(depth);
    ++pos_;
    const auto start = builder_.writePosition();
    std::vector<uint64_t> offsets;
    skipWhitespace();
    if (!atEnd() && peek() == ']') {
      ++pos_;
      builder_.finishArray(start, offsets);
      return;
    }
    while (true) {
      skipWhitespace();
      offsets.push_back(builder_.writePosition() - start);
      readValue(depth, /*isRoot=*/false);
      skipWhitespace();
      if (atEnd()) {
        fail("Unexpected end of input");
      }
      if (peek() == ',') {
        ++pos_;
        continue;
      }
      if (peek() == ']') {
        ++pos_;
        break;
      }
      fail("Expected ',' or ']'");
    }
    builder_.finishArray(start, offsets);
  }

  static bool isIdentifierPart(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9') || c == '_' || c == '$' ||
        static_cast<uint8_t>(c) >= 0x80;
  }

  void readLiteral(std::string_view literal) {
    if (json_.substr(pos_, literal.size()) != literal) {
      fail("Unrecognized token");
    }
    pos_ += literal.size();
    // Jackson rejects tokens such as 'truex'.
    if (!atEnd() && isIdentifierPart(peek())) {
      fail("Unrecognized token");
    }
  }

  uint32_t readHex4() {
    if (pos_ + 4 > json_.size()) {
      fail("Invalid unicode escape");
    }
    uint32_t value{0};
    for (int i = 0; i < 4; ++i) {
      const char c = json_[pos_++];
      value <<= 4;
      if (c >= '0' && c <= '9') {
        value |= c - '0';
      } else if (c >= 'a' && c <= 'f') {
        value |= c - 'a' + 10;
      } else if (c >= 'A' && c <= 'F') {
        value |= c - 'A' + 10;
      } else {
        fail("Invalid unicode escape");
      }
    }
    return value;
  }

  static void appendUtf8(uint32_t codePoint, std::string& out) {
    if (codePoint < 0x80) {
      out.push_back(static_cast<char>(codePoint));
    } else if (codePoint < 0x800) {
      out.push_back(static_cast<char>(0xC0 | (codePoint >> 6)));
      out.push_back(static_cast<char>(0x80 | (codePoint & 0x3F)));
    } else if (codePoint < 0x10000) {
      out.push_back(static_cast<char>(0xE0 | (codePoint >> 12)));
      out.push_back(static_cast<char>(0x80 | ((codePoint >> 6) & 0x3F)));
      out.push_back(static_cast<char>(0x80 | (codePoint & 0x3F)));
    } else {
      out.push_back(static_cast<char>(0xF0 | (codePoint >> 18)));
      out.push_back(static_cast<char>(0x80 | ((codePoint >> 12) & 0x3F)));
      out.push_back(static_cast<char>(0x80 | ((codePoint >> 6) & 0x3F)));
      out.push_back(static_cast<char>(0x80 | (codePoint & 0x3F)));
    }
  }

  static bool isHighSurrogate(uint32_t unit) {
    return unit >= 0xD800 && unit <= 0xDBFF;
  }

  static bool isLowSurrogate(uint32_t unit) {
    return unit >= 0xDC00 && unit <= 0xDFFF;
  }

  // Reads a unicode escape whose 'u' is at the current position, combining a
  // surrogate pair. Spark encodes the parsed Java string as UTF-8, which turns
  // an unpaired surrogate into '?'.
  void readUnicodeEscape(std::string& out) {
    ++pos_;
    const auto unit = readHex4();
    if (isHighSurrogate(unit) && json_.substr(pos_, 2) == "\\u") {
      const auto savedPos = pos_;
      pos_ += 2;
      const auto low = readHex4();
      if (isLowSurrogate(low)) {
        appendUtf8(0x10000 + ((unit - 0xD800) << 10) + (low - 0xDC00), out);
        return;
      }
      pos_ = savedPos;
    }
    if (isHighSurrogate(unit) || isLowSurrogate(unit)) {
      out.push_back('?');
      return;
    }
    appendUtf8(unit, out);
  }

  void readString(std::string& out) {
    ++pos_;
    while (true) {
      if (atEnd()) {
        fail("Unterminated string");
      }
      const char c = json_[pos_];
      if (c == '"') {
        ++pos_;
        return;
      }
      if (static_cast<uint8_t>(c) < 0x20) {
        fail("Illegal unquoted control character in string");
      }
      if (c != '\\') {
        out.push_back(c);
        ++pos_;
        continue;
      }
      ++pos_;
      if (atEnd()) {
        fail("Unterminated string");
      }
      switch (json_[pos_]) {
        case '"':
          out.push_back('"');
          break;
        case '\\':
          out.push_back('\\');
          break;
        case '/':
          out.push_back('/');
          break;
        case 'b':
          out.push_back('\b');
          break;
        case 'f':
          out.push_back('\f');
          break;
        case 'n':
          out.push_back('\n');
          break;
        case 'r':
          out.push_back('\r');
          break;
        case 't':
          out.push_back('\t');
          break;
        case 'u':
          readUnicodeEscape(out);
          continue;
        default:
          fail("Unrecognized character escape");
      }
      ++pos_;
    }
  }

  static bool isDigit(char c) {
    return c >= '0' && c <= '9';
  }

  void readDigits() {
    if (atEnd() || !isDigit(peek())) {
      fail("Expected a digit");
    }
    while (!atEnd() && isDigit(peek())) {
      ++pos_;
    }
  }

  void readNumber(bool isRoot) {
    const auto start = pos_;
    bool isInteger{true};
    if (peek() == '-') {
      ++pos_;
    }
    if (atEnd() || !isDigit(peek())) {
      fail("Unexpected character");
    }
    if (peek() == '0') {
      ++pos_;
      if (!atEnd() && isDigit(peek())) {
        fail("Leading zeroes are not allowed");
      }
    } else {
      readDigits();
    }
    if (!atEnd() && peek() == '.') {
      isInteger = false;
      ++pos_;
      readDigits();
    }
    bool hasExponent{false};
    if (!atEnd() && (peek() == 'e' || peek() == 'E')) {
      isInteger = false;
      hasExponent = true;
      ++pos_;
      if (!atEnd() && (peek() == '+' || peek() == '-')) {
        ++pos_;
      }
      readDigits();
    }
    const auto text = json_.substr(start, pos_ - start);
    if (text.size() > kMaxNumberLength) {
      fail("Number length exceeds the maximum");
    }
    // Jackson requires whitespace after a top-level number.
    if (isRoot && !atEnd()) {
      const char c = peek();
      if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
        fail("Expected space separating root-level values");
      }
    }

    if (isInteger) {
      if (auto value = folly::tryTo<int64_t>(text); value.hasValue()) {
        builder_.appendLong(value.value());
        return;
      }
    }
    if (!hasExponent && tryAppendDecimal(text)) {
      return;
    }
    appendDouble(text);
  }

  // Appends 'text', which has no exponent, as a decimal if its precision and
  // scale are at most 38.
  bool tryAppendDecimal(std::string_view text) {
    const bool negative = text.front() == '-';
    if (negative) {
      text.remove_prefix(1);
    }
    const auto point = text.find('.');
    const auto integerPart = text.substr(0, point);
    const auto fractionPart = point == std::string_view::npos
        ? std::string_view{}
        : text.substr(point + 1);
    const auto scale = static_cast<int32_t>(fractionPart.size());

    std::string digits{integerPart};
    digits.append(fractionPart);
    const auto firstNonZero = digits.find_first_not_of('0');
    const auto precision = firstNonZero == std::string::npos
        ? 1
        : static_cast<int32_t>(digits.size() - firstNonZero);
    if (precision > kMaxDecimalPrecision || scale > kMaxDecimalPrecision) {
      return false;
    }

    int128_t unscaled{0};
    for (auto c : digits) {
      unscaled = unscaled * 10 + (c - '0');
    }
    builder_.appendDecimal(
        negative ? -unscaled : unscaled, static_cast<uint8_t>(scale));
    return true;
  }

  void appendDouble(std::string_view text) {
    // Like Java's Double.parseDouble, values out of range become infinity.
    const std::string terminated{text};
    builder_.appendDouble(std::strtod(terminated.c_str(), nullptr));
  }

  const std::string_view json_;
  VariantBuilder& builder_;
  size_t pos_{0};
};

void appendQuoted(std::string_view value, std::string& out) {
  out.push_back('"');
  out.append(value);
  out.push_back('"');
}

void appendEscaped(std::string_view value, std::string& out) {
  folly::json::escapeString(value, out, folly::json::serialization_opts{});
}

template <typename T>
void appendFloatingPoint(T value, std::string& out) {
  const auto text = util::Converter<TypeKind::VARCHAR>::tryCast(value).value();
  if (std::isfinite(value)) {
    out.append(text);
  } else {
    appendQuoted(text, out);
  }
}

void appendDateTime(std::tm& calendar, int64_t micros, std::string& out) {
  out.append(
      fmt::format(
          "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
          calendar.tm_year + 1900,
          calendar.tm_mon + 1,
          calendar.tm_mday,
          calendar.tm_hour,
          calendar.tm_min,
          calendar.tm_sec));
  auto fraction = micros % 1'000'000;
  if (fraction < 0) {
    fraction += 1'000'000;
  }
  if (fraction != 0) {
    auto digits = fmt::format("{:06}", fraction);
    digits.erase(digits.find_last_not_of('0') + 1);
    out.push_back('.');
    out.append(digits);
  }
}

} // namespace

void VariantJson::parse(std::string_view json, VariantBuilder& builder) {
  JsonReader(json, builder).read();
}

void VariantJson::appendDecimal(
    const VariantDecimal& decimal,
    std::string& out) {
  const bool negative = decimal.unscaledValue < 0;
  auto magnitude = negative ? -static_cast<uint128_t>(decimal.unscaledValue)
                            : static_cast<uint128_t>(decimal.unscaledValue);
  std::string digits;
  do {
    digits.push_back(static_cast<char>('0' + magnitude % 10));
    magnitude /= 10;
  } while (magnitude > 0);
  while (digits.size() <= decimal.scale) {
    digits.push_back('0');
  }
  std::reverse(digits.begin(), digits.end());

  const auto integerSize = digits.size() - decimal.scale;
  auto fraction = digits.substr(integerSize);
  fraction.erase(fraction.find_last_not_of('0') + 1);
  const bool isZero = digits.find_first_not_of('0') == std::string::npos;
  if (negative && !isZero) {
    out.push_back('-');
  }
  out.append(digits, 0, integerSize);
  if (!fraction.empty()) {
    out.push_back('.');
    out.append(fraction);
  }
}

void VariantJson::appendTimestamp(
    int64_t micros,
    const tz::TimeZone* timeZone,
    std::string& out) {
  const int64_t seconds =
      micros >= 0 ? micros / 1'000'000 : (micros - 999'999) / 1'000'000;
  int64_t localSeconds = seconds;
  if (timeZone != nullptr) {
    localSeconds = timeZone->to_local(std::chrono::seconds(seconds)).count();
  }
  std::tm calendar;
  VELOX_USER_CHECK(
      Timestamp::epochToCalendarUtc(localSeconds, calendar),
      "Timestamp is out of range: {}",
      micros);
  appendDateTime(calendar, micros, out);
  if (timeZone == nullptr) {
    return;
  }
  const auto offsetMinutes = (localSeconds - seconds) / 60;
  const auto absMinutes = std::abs(offsetMinutes);
  out.append(
      fmt::format(
          "{}{:02}:{:02}",
          offsetMinutes < 0 ? '-' : '+',
          absMinutes / 60,
          absMinutes % 60));
}

void VariantJson::appendUuid(std::string_view bytes, std::string& out) {
  static constexpr char kHex[] = "0123456789abcdef";
  for (size_t i = 0; i < bytes.size(); ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) {
      out.push_back('-');
    }
    const auto byte = static_cast<uint8_t>(bytes[i]);
    out.push_back(kHex[byte >> 4]);
    out.push_back(kHex[byte & 0x0F]);
  }
}

void VariantJson::toJson(
    const VariantValue& value,
    const tz::TimeZone* timeZone,
    std::string& out) {
  switch (value.kind()) {
    case VariantKind::kObject:
      out.push_back('{');
      for (uint32_t i = 0; i < value.numElements(); ++i) {
        if (i > 0) {
          out.push_back(',');
        }
        appendEscaped(value.fieldNameAt(i), out);
        out.push_back(':');
        toJson(value.fieldValueAt(i), timeZone, out);
      }
      out.push_back('}');
      break;
    case VariantKind::kArray:
      out.push_back('[');
      for (uint32_t i = 0; i < value.numElements(); ++i) {
        if (i > 0) {
          out.push_back(',');
        }
        toJson(value.elementAt(i), timeZone, out);
      }
      out.push_back(']');
      break;
    case VariantKind::kNull:
      out.append("null");
      break;
    case VariantKind::kBoolean:
      out.append(value.asBoolean() ? "true" : "false");
      break;
    case VariantKind::kInt8:
    case VariantKind::kInt16:
    case VariantKind::kInt32:
    case VariantKind::kInt64:
      out.append(std::to_string(value.asInteger()));
      break;
    case VariantKind::kString:
      appendEscaped(value.asString(), out);
      break;
    case VariantKind::kDouble:
      appendFloatingPoint(value.asDouble(), out);
      break;
    case VariantKind::kFloat:
      appendFloatingPoint(value.asFloat(), out);
      break;
    case VariantKind::kDecimal4:
    case VariantKind::kDecimal8:
    case VariantKind::kDecimal16:
      appendDecimal(value.asDecimal(), out);
      break;
    case VariantKind::kDate:
      appendQuoted(DATE()->toString(value.asDate()), out);
      break;
    case VariantKind::kTimestamp: {
      std::string text;
      appendTimestamp(
          value.asTimestamp(),
          timeZone != nullptr ? timeZone : tz::locateZone("UTC"),
          text);
      appendQuoted(text, out);
      break;
    }
    case VariantKind::kTimestampNtz: {
      std::string text;
      appendTimestamp(value.asTimestamp(), nullptr, text);
      appendQuoted(text, out);
      break;
    }
    case VariantKind::kBinary: {
      const auto binary = value.asBinary();
      appendQuoted(encoding::Base64::encode(binary.data(), binary.size()), out);
      break;
    }
    case VariantKind::kUuid: {
      std::string text;
      appendUuid(value.asUuid(), text);
      appendQuoted(text, out);
      break;
    }
    default:
      VELOX_USER_FAIL(
          "Variant value kind is not supported by Spark: {}", value.kind());
  }
}

} // namespace facebook::velox::functions::sparksql
