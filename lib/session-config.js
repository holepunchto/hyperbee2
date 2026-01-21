class SessionConfig {
  constructor(activeRequests, timeout, wait) {
    this.activeRequests = activeRequests
    this.timeout = timeout
    this.wait = wait
  }

  sub(activeRequests, timeout, wait) {
    if (this.activeRequests === activeRequests && this.timeout === timeout && this.wait === wait) {
      return this
    }

    return new SessionConfig(activeRequests, timeout, wait)
  }

  options(opts) {
    if (!opts) return this
    const { activeRequests = this.activeRequests, timeout = this.timeout, wait = this.wait } = opts
    return this.sub(activeRequests, timeout, wait)
  }
}

module.exports = SessionConfig
