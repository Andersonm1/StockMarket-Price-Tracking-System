// Reference data for the default watchlist. Base prices only seed the simulator;
// they are illustrative, not real quotes. `volatility` is the per-tick standard deviation.
export const COMPANIES = [
  { symbol: 'AAPL', name: 'Apple Inc.', sector: 'Technology', basePrice: 228, volatility: 0.0012 },
  { symbol: 'MSFT', name: 'Microsoft Corporation', sector: 'Technology', basePrice: 430, volatility: 0.0010 },
  { symbol: 'GOOGL', name: 'Alphabet Inc.', sector: 'Communication Services', basePrice: 175, volatility: 0.0013 },
  { symbol: 'AMZN', name: 'Amazon.com, Inc.', sector: 'Consumer Discretionary', basePrice: 195, volatility: 0.0014 },
  { symbol: 'NVDA', name: 'NVIDIA Corporation', sector: 'Technology', basePrice: 120, volatility: 0.0022 },
  { symbol: 'META', name: 'Meta Platforms, Inc.', sector: 'Communication Services', basePrice: 560, volatility: 0.0016 },
  { symbol: 'TSLA', name: 'Tesla, Inc.', sector: 'Consumer Discretionary', basePrice: 240, volatility: 0.0028 },
  { symbol: 'JPM', name: 'JPMorgan Chase & Co.', sector: 'Financials', basePrice: 215, volatility: 0.0009 },
  { symbol: 'V', name: 'Visa Inc.', sector: 'Financials', basePrice: 285, volatility: 0.0008 },
  { symbol: 'JNJ', name: 'Johnson & Johnson', sector: 'Health Care', basePrice: 160, volatility: 0.0007 },
  { symbol: 'WMT', name: 'Walmart Inc.', sector: 'Consumer Staples', basePrice: 80, volatility: 0.0008 },
  { symbol: 'XOM', name: 'Exxon Mobil Corporation', sector: 'Energy', basePrice: 115, volatility: 0.0011 },
];

/** Returns the watchlist, restricted to `symbols` if given. Unknown symbols get placeholder metadata. */
export function resolveCompanies(symbols) {
  if (!symbols || symbols.length === 0) return COMPANIES;
  return symbols.map((raw) => {
    const symbol = raw.toUpperCase();
    return COMPANIES.find((c) => c.symbol === symbol)
      ?? { symbol, name: symbol, sector: 'Unknown', basePrice: 100, volatility: 0.0012 };
  });
}
