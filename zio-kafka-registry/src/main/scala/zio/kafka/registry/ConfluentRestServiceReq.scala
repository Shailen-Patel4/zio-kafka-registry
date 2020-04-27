package zio.kafka.registry

import zio.{Semaphore, Has, ZLayer, UIO}


/**
 * Wrapper for RestService config settings requirements
 */
package object confluentRestServiceReq {

  type ConfluentRestServiceReq = Has[ConfluentRestServiceReq.Service]
  object ConfluentRestServiceReq {
    trait Service {
      val sem: UIO[Semaphore]
    }
    def live: ZLayer.NoDeps[Nothing, ConfluentRestServiceReq] = ZLayer.succeed(
      new Service {
        override val sem: UIO[Semaphore] = Semaphore.make(1)
      }
    )
  }
}


