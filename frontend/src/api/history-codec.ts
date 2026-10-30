import {
  historyPageValidator,
  rawHistoryPageValidator,
  chartValidator,
  exportJobValidator,
} from "@maintai/validators";
import { tokens, plain, object, integer } from "./codec";
import type {
  Collection,
  HistoryPage,
  Chart,
  ExportJob,
} from "../types/history";

function exact(value: unknown, validation: boolean, key = ""): unknown {
  if (key === "features_raw") return validation ? plain(value) : value;
  if (
    key === "sequence_number" ||
    key === "offset" ||
    key.endsWith("_offset")
  ) {
    if (value === null) return null;
    const result = integer(value, key !== "sequence_number");
    return validation ? (key === "sequence_number" ? 1 : 0) : result;
  }
  if (Array.isArray(value)) return value.map((item) => exact(item, validation));
  if (object(value))
    return Object.fromEntries(
      Object.entries(value).map(([name, item]) => [
        name,
        exact(item, validation, name),
      ]),
    );
  return plain(value);
}
function parse<T>(text: string, validator: (v: unknown) => boolean): T {
  const value = tokens(text);
  if (!validator(exact(value, true)))
    throw new Error("Phản hồi lịch sử không đúng contract M6.");
  return exact(value, false) as T;
}
export function parseHistory(
  text: string,
  collection: Collection,
): HistoryPage {
  const value = parse<HistoryPage>(
    text,
    collection === "PROCESSING"
      ? historyPageValidator
      : rawHistoryPageValidator,
  );
  if (value.collection !== collection)
    throw new Error("Sai nguồn dữ liệu lịch sử.");
  return value;
}
export const parseChart = (text: string) => parse<Chart>(text, chartValidator);
export const parseExportJob = (text: string) =>
  parse<ExportJob>(text, exportJobValidator);
