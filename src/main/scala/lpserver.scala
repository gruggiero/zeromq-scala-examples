/*
 *  Lazy Pirate server
 *  Binds REQ socket to tcp:// *:5555
 *  Like hwserver except:
 *   - echoes request as-is
 *   - randomly runs slowly, or exits to simulate a crash.
 *
 *
 *  @Author:     Giovanni Ruggiero
 *  @Email:      giovanni.ruggiero@gmail.com
 */

import util.control.Breaks._
import org.zeromq.ZMQ
import ZHelpers._

object lpserver {
	def main(args : Array[String]) {
		val rand = new java.util.Random(System.currentTimeMillis)
		val context = ZMQ.context(1)
		val socket = context.socket(ZMQ.REP)
		socket.bind ("tcp://*:5555")

		var cycles = 0
		breakable {
			while (true) {
				val request = socket.recv (0)
				cycles += 1
				//  Simulate various problems, after a few cycles
        if (cycles > 3 && rand.nextInt() % 5 == 0) {
          println ("I: simulating a crash\n")
          break
        } else if (cycles > 3 && rand.nextInt() % 5 != 0) {
          println ("I: simulating CPU overload")
          Thread.sleep (2000)
        }
        printf ("I: normal request (%s)\n", new String(request));
        Thread.sleep (1000)  //  Do some heavy work
				socket.send(request, 0);
			}
		}
	}
}
