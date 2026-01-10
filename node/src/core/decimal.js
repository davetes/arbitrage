import Decimal from 'decimal.js';

// Configure to be conservative like Python Decimal usage
Decimal.set({ precision: 40, rounding: Decimal.ROUND_DOWN });

export { Decimal };

export function d(x) {
  // Always convert via string to avoid float binary issues
  if (x instanceof Decimal) return x;
  return new Decimal(String(x));
}

export function floorToStepDecimal(qty, step) {
  const q = d(qty);
  const s = d(step);
  if (s.lte(0)) return q;
  if (q.lte(0)) return d(0);
  const steps = q.div(s).floor();
  return steps.mul(s);
}

export function quantize8(x) {
  // equivalent to quantize(0.00000001) ROUND_DOWN
  return d(x).toDecimalPlaces(8, Decimal.ROUND_DOWN);
}
