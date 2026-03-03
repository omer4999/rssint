/**
 * Time formatting utilities.
 * All display times are shown in the user's LOCAL timezone via Intl.DateTimeFormat.
 */

const _localFormatter = new Intl.DateTimeFormat(undefined, {
  year: "2-digit",
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

/**
 * Format an ISO-8601 string into a compact local-time string,
 * e.g. "02 Mar 26, 14:35:07".
 *
 * Uses the browser's locale and timezone automatically.
 */
export function formatLocalTime(iso: string): string {
  return _localFormatter.format(new Date(iso));
}

/**
 * Format a Date object as a local time string — used for the "last updated"
 * indicator in the Header which receives a Date, not an ISO string.
 */
export function formatLocalDate(date: Date): string {
  return _localFormatter.format(date);
}
