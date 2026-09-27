// Dumps canonicalJson goldens for an adversarial corpus, for the Rust parity test (tests/json.rs).
//   node crates/flower-client/parity/canonical.ts > crates/flower-client/tests/vectors/canonical.json
// Each case: the input as JSON text, canonicalJson(JSON.parse(input)), the UTF-8 byte length of
// JSON.stringify(JSON.parse(input)), and for a number, the IEEE-754 bits JSON.parse chose (so Rust
// can check formatting independently of serde_json's float parsing, which rounds exactly only with
// its float_roundtrip feature). Random decimal cases use a fixed seed, so the file is stable.
import { canonicalJson } from "../../../sdk/json.ts";

const inputs: string[] = [
  // Literals and strings: escapes, controls, U+2028/2029, DEL, astral characters, private use.
  "null", "true", "false", '""', '"flower"', '"a\\u0000b🌸"', '"\\u2028\\u2029"', '"\\u007f"',
  '"\\u0001\\u001f\\b\\f\\n\\r\\t\\u000b"', '"\\"\\\\/"', '"\\ud83d\\ude00"', '"\\uffff\\ue000"', '"é e\\u0301"',
  JSON.stringify(Array.from({ length: 32 }, (_, code) => String.fromCharCode(code)).join("")),
  // Numbers: zero signs, exponent thresholds, precision, extremes, integers beyond 2^53.
  "0", "-0", "-0.0", "1", "-1", "1.5", "1.0", "1.50", "0.1", "0.2", "0.30000000000000004", "4.35", "0.1e1", "1E+2", "1e2",
  "1e21", "1e20", "999999999999999999999", "99999999999999990000", "100000000000000000000", "123e-20",
  "1e-7", "1e-6", "0.000001", "0.0000001", "-1e-7", "1e300", "1e308", "1.7976931348623157e308", "5e-324",
  "4.9406564584124654e-324", "2.2250738585072014e-308", "2.2250738585072011e-308", "1.1754943508222875e-38",
  "8.98846567431158e307", "1.2345678901234567e-300", "9007199254740991", "9007199254740992", "9007199254740993",
  "-9007199254740993", "12345678901234567890", "18446744073709551615", "18446744073709551616",
  "123456789012345678901234567890", "-123456789012345678901234567890", "0.1234567890123456789",
  "[0,-0,1e21,1e-7,5e-324,9007199254740993]",
  // Keys: UTF-16 order (not UTF-8, not numeric), integer-like keys, empty, prototype-looking, nesting.
  '{"b":1,"a":2}', '{"10":10,"2":2,"😀":2,"\\ue000":1,"":0}', '{"\\uffff":1,"😀":2,"\\ue000":3,"~":4}',
  '{"a":{"c":1,"b":[{"z":1,"y":2},{"b":{"d":[],"c":{}}}]}}', '{"é":1,"e\\u0301":2,"z":3,"E":4}',
  '{"__proto__":{"x":1},"constructor":{"prototype":1},"toString":2}', '{"1":1,"01":2,"1.5":3,"-1":4,"a":5,"A":6,"_":7}',
  '{"\\u0000":1,"\\u001f":2," ":3,"\\"":4,"\\\\":5}', '[[],{},[{}],{"a":[]}]', "[]", "{}", '[[[[["deep"]]]]]',
  JSON.stringify(Array.from({ length: 127 }).reduce<unknown>((value) => [value], ["key"])),
  JSON.stringify(Array.from({ length: 60 }).reduce<unknown>((value, _, index) => ({ [`k${index % 7}`]: value, z: index }), 0)),
];

// Seeded random decimals: 17-20 significant digits and wide exponents stress float parsing.
let seed = 0x2545f491;
const random = () => ((seed = (seed * 1103515245 + 12345) >>> 0) / 2 ** 32);
for (let index = 0; index < 400; index++) {
  const digits = 1 + Math.floor(random() * 20);
  let mantissa = String(1 + Math.floor(random() * 9));
  for (let digit = 1; digit < digits; digit++) mantissa += Math.floor(random() * 10);
  const point = Math.floor(random() * (digits + 1));
  const decimal = point === digits ? mantissa : `${mantissa.slice(0, point) || "0"}.${mantissa.slice(point)}`;
  const exponent = Math.floor(random() * 640) - 330;
  inputs.push(`${random() < 0.2 ? "-" : ""}${decimal}${index % 3 === 0 ? "" : `e${exponent}`}`);
}

const cases = inputs.flatMap((input) => {
  const value = JSON.parse(input);
  if (typeof value === "number" && !Number.isFinite(value)) return [];
  const bits = typeof value === "number" ? Buffer.from(new Float64Array([value]).buffer).reverse().toString("hex") : undefined;
  return [{ input, canonical: canonicalJson(value), stringifyBytes: Buffer.byteLength(JSON.stringify(value)), ...(bits ? { bits } : {}) }];
});
process.stdout.write(JSON.stringify(cases, null, 1) + "\n");
