import { Table } from "apache-arrow";
import { Buffer } from "node:buffer";

import { normalizeInput, unwrapNativeValue } from "./conversion.js";
import { conversionError, XqdbError, rejectionToIOError } from "./errors.js";
import { loadNativeBinding } from "./native-loader.js";
import type { NativeModule, NativeResult } from "./native-contract.js";
import { validatedSymbolEncoding } from "./types.js";
import type { XqdbInput, XqdbMessageType, XqdbSymbolEncoding } from "./types.js";

export interface ReadBinary6Options {
  /** Decoding policy for symbols and strings that are not valid UTF-8; defaults to `"strict"`. */
  readonly symbolEncoding?: XqdbSymbolEncoding;
}

async function invokeNativeHelper(
  operation: (binding: NativeModule) => Promise<NativeResult>,
): Promise<NativeResult> {
  try {
    const binding = await loadNativeBinding();
    return await operation(binding);
  } catch (cause) {
    if (cause instanceof XqdbError) {
      throw cause;
    }
    throw rejectionToIOError(cause);
  }
}

export async function readBinary6(
  path: string,
  options: ReadBinary6Options = {},
): Promise<Table> {
  const symbolEncoding = validatedSymbolEncoding(options.symbolEncoding, "ReadBinary6Options");
  const result = await invokeNativeHelper((binding) => binding.readBinary6(path, symbolEncoding));
  const value = unwrapNativeValue(result);
  if (!(value instanceof Table)) {
    throw conversionError("readBinary6 returned a native value that was not a table");
  }
  return value;
}

export async function serializeAsIpcBytes6(
  messageType: XqdbMessageType,
  compress: boolean,
  value: XqdbInput,
): Promise<Buffer> {
  const nativeValue = normalizeInput(value);
  const result = await invokeNativeHelper((binding) =>
    binding.serializeAsIpcBytes6(messageType, compress, nativeValue),
  );
  const bytes = unwrapNativeValue(result);
  if (!Buffer.isBuffer(bytes)) {
    throw conversionError("serializeAsIpcBytes6 returned a native value that was not bytes");
  }
  return bytes;
}
