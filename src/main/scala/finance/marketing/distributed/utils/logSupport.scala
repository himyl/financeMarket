package finance.marketing.distributed.utils

import org.slf4j.LoggerFactory

trait logSupport {

  /**
   * Logger
   */
  protected val log = LoggerFactory.getLogger(this.getClass)

}
