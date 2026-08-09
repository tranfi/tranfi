class TranfiTransformError extends Error {
  constructor(code, message, cause) {
    super(message)
    this.name = 'TranfiTransformError'
    this.code = Number(code)
    if (cause !== undefined) this.cause = cause
  }
}

module.exports = { TranfiTransformError }
