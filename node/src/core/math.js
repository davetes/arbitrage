export function floorToStep(qty, step) {
  const q = Number(qty);
  const s = Number(step);
  if (!(q > 0) || !(s > 0)) return 0;
  const steps = Math.floor(q / s + 1e-12);
  // avoid floating errors by rounding to step decimals
  const decimals = Math.max(0, (String(step).split('.')[1] || '').length);
  return Number((steps * s).toFixed(decimals));
}
