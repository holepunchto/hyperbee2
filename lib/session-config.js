class SessionConfig {
  constructor(activeRequests, timeout, wait, trace) {
    this.activeRequests = activeRequests
    this.timeout = timeout
    this.wait = wait
    this.trace = trace
  }

  sub(activeRequests, timeout, wait, trace) {
    if (
      this.activeRequests === activeRequests &&
      this.timeout === timeout &&
      this.wait === wait &&
      this.trace === trace
    ) {
      return this
    }

    return new SessionConfig(activeRequests, timeout, wait, trace)
  }

  options(opts) {
    if (!opts) return this
    const {
      activeRequests = this.activeRequests,
      timeout = this.timeout,
      wait = this.wait,
      trace = this.trace
    } = opts
    return this.sub(activeRequests, timeout, wait, trace)
  }

  detach(opts) {
    if (!opts) return this.sub([], this.timeout, this.wait, this.trace)
    const {
      activeRequests = [],
      timeout = this.timeout,
      wait = this.wait,
      trace = this.trace
    } = opts
    return this.sub(activeRequests, timeout, wait, trace)
  }
}

module.exports = SessionConfig
