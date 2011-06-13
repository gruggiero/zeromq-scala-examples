/*
 *  Simple Pirate worker
 *  Connects REQ socket to tcp:// *:5556
 *  Implements worker part of LRU queueing
 *
 *
 *  @Author:     Giovanni Ruggiero
 *  @Email:      giovanni.ruggiero@gmail.com
 */

import org.zeromq.ZMQ
import ZHelpers._

object spworker {
	def main(args : Array[String]) {
		val Ready = "\001" getBytes

		val rand = new java.util.Random(System.currentTimeMillis)
		val ctx = ZMQ.context(1)
		val worker = ctx.socket(ZMQ.REQ)
		setID(worker)
		worker.connect("tcp://localhost:5556")
    val identity = new String(worker getIdentity)
    //  Tell broker we're ready for work
    printf ("I: (%s) worker ready\n", identity)
		worker.send(Ready, 0)
		var cycles = 0
		var stopped = false
		do {
			val msg = worker recvMsg

			cycles += 1
			//  Simulate various problems, after a few cycles
      if (cycles > 3 && rand.nextInt() % 5 == 0) {
        printf ("I: (%s) simulating a crash\n", identity)
        stopped = true
      } else if (cycles > 3 && rand.nextInt() % 5 != 0) {
        printf ("I: (%s) simulating CPU overload\n", identity)
        Thread.sleep (3000)
      }
      printf ("I: (%s) normal reply\n", identity);
      Thread.sleep (1000)  //  Do some heavy work
			worker.sendMsg(msg)
		} while (!stopped)
	}
}
