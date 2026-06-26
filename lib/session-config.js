class SessionConfig {
  constructor(activeRequests, timeout, wait, trace, localOnly = false) {
    this.activeRequests = activeRequests
    this.timeout = timeout
    this.wait = wait
    this.trace = trace
    this.localOnly = localOnly
  }

  sub(activeRequests, timeout, wait, trace, localOnly) {
    if (
      this.activeRequests === activeRequests &&
      this.timeout === timeout &&
      this.wait === wait &&
      this.trace === trace &&
      this.localOnly === localOnly
    ) {
      return this
    }

    return new SessionConfig(activeRequests, timeout, wait, trace, localOnly)
  }

  options(opts) {
    if (!opts) return this
    let {
      activeRequests = this.activeRequests,
      timeout = this.timeout,
      wait = this.wait,
      trace = this.trace,
      localOnly = this.localOnly
    } = opts

    if (localOnly) wait = false

    return this.sub(activeRequests, timeout, wait, trace, localOnly)
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
