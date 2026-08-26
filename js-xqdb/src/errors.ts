import type { NativeError } from "./native-contract.js";

export interface XqdbErrorOptions {
  readonly cause?: unknown;
}

export class XqdbError extends Error {
  public readonly code: string;
  public readonly nativeMessage: string;

  public constructor(code: string, nativeMessage: string, options?: XqdbErrorOptions) {
    super(nativeMessage, options);
    this.name = "XqdbError";
    this.code = code;
    this.nativeMessage = nativeMessage;
  }
}

export class XqdbIOError extends XqdbError {
  public constructor(code: string, nativeMessage: string, options?: XqdbErrorOptions) {
    super(code, nativeMessage, options);
    this.name = "XqdbIOError";
  }
}

export class XqdbAuthError extends XqdbError {
  public constructor(code: string, nativeMessage: string, options?: XqdbErrorOptions) {
    super(code, nativeMessage, options);
    this.name = "XqdbAuthError";
  }
}

export function mapNativeError(error: NativeError): XqdbError {
  const options: XqdbErrorOptions = { cause: error };
  if (error.code === "XQDB_IO") {
    return new XqdbIOError(error.code, error.message, options);
  }
  if (error.code === "XQDB_AUTH") {
    return new XqdbAuthError(error.code, error.message, options);
  }
  return new XqdbError(error.code, error.message, options);
}

export function rejectionToIOError(cause: unknown): XqdbIOError {
  if (cause instanceof XqdbIOError) {
    return cause;
  }
  const message = cause instanceof Error ? cause.message : String(cause);
  return new XqdbIOError("XQDB_IO", message, { cause });
}

export function conversionError(message: string, cause?: unknown): XqdbError {
  return new XqdbError("XQDB_CONVERSION", message, cause === undefined ? undefined : { cause });
}
