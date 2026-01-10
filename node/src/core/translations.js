export function t(key, lang = 'en') {
  // Python only had English currently.
  const en = {
    check: 'Check Validity',
    exec: 'Execute Trade',
    start_search: 'Start Search',
    stop_search: 'Stop Search',
  };
  return (lang === 'en' ? en[key] : en[key]) || key;
}
