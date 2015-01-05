

import akka.actor.Actor
import akka.actor.Props
import akka.actor.Props
import akka.actor.ActorSystem
import scala.util.Random
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.actor.PoisonPill

object Project2bonus {

  sealed trait Simulator
  case class Start() extends Simulator
  case class Gossip(origin: String, message: String) extends Simulator
  case class PushSum(s: Double, w: Double) extends Simulator
  case class GossipDone(source: String) extends Simulator
  case class PushSumDone(x: Double) extends Simulator
  case class FailNode() extends Simulator

  val END_GOSSIP: Int = 10;
  val numofMessages: Int = 5;
  val END_PUSHSUM: Int = 3;

  def main(args: Array[String]) {

    if (args.length != 3) {
      println("ERROR: Invalid Input ! Enter the input as <numNodes> <Topology> <Algorithm> ! Exiting ...."); System.exit(1);
    }

    var numNodes = args(0).toInt
    var topology: String = args(1).toString
    if (args(1).compareToIgnoreCase("full") == 0) topology = "Full"
    else if (args(1).compareToIgnoreCase("2d") == 0) topology = "Grid2d"
    else if (args(1).compareToIgnoreCase("line") == 0) topology = "Line"
    else if (args(1).compareToIgnoreCase("imp2d") == 0) topology = "Imp2d"
    else {
      println("ERROR: Invalid Topology selected for simulation ! Exiting ...");
      System.exit(1);
      }

    

    var algorithm: String = args(2).toString()
    if (args(2).compareToIgnoreCase("gossip") == 0) algorithm = "Gossip"
    else if (args(2).compareToIgnoreCase("push-sum") == 0) algorithm = "PushSum"
    else {
      println("ERROR: Invalid Algorithm selected for simulation ! Exiting ...");
      System.exit(1); }
    println("Running simulation for Nodes: " + numNodes + " Topology: " + topology + " Algorithm: " + algorithm)

    val system = ActorSystem("GossipSimulator")
    val master = system.actorOf(Props(new Master(numNodes, topology, algorithm)), "master")

    master ! Start
  }

  class Master(nodecnt: Int, topology: String, algorithm: String) extends Actor {

    val message: String = "Gossip"
    var gridsize: Int = 0
    var startTime: Long = 0
    var doneCount: Int = 0
    var Workerlist = new ArrayBuffer[String]
    var numNodes: Int = nodecnt
    //val Failurepercent: Int = Random.nextInt(100)
    val Failurepercent: Int = 50
    var killNode: String = "NULL"
    var killit: String = "true"
    if (topology == "Grid2d" || topology == "Imp2d") {
      gridsize = math.sqrt(nodecnt).toInt
      while ((gridsize * gridsize) != numNodes) {
        numNodes = numNodes + 1
        gridsize = math.sqrt(numNodes).toInt

      }
      if (numNodes != nodecnt) println("Number of nodes increased to " + numNodes + " for maintaining grid property")
    }

    def receive = {

      case Start =>
        if (topology == "Grid2d" || topology == "Imp2d") {

          
          for (a <- 0 to gridsize.toInt)
            for (b <- 0 to gridsize.toInt) {
              
              context.actorOf(Props(new Worker(a, b, algorithm, topology, numNodes)), (a.toString.concat(".")).concat(b.toString));
            }

        }
        if (topology == "Full" || topology == "Line") {
          val b: Int = 0;
         
          for (a <- 0 to numNodes) {
            
            context.actorOf(Props(new Worker(a, b, algorithm, topology, numNodes)), (a.toString.concat(".")).concat("0"));
	  
          }
          
        }
        startTime = System.currentTimeMillis
        algorithm match {
          case "Gossip" =>

            if (topology == "Grid2d" || topology == "Imp2d") {
              var origin: String = (Random.nextInt(gridsize.toInt).toString.concat(".")).concat(Random.nextInt(gridsize.toInt).toString)
              context.actorSelection(origin) ! Gossip(origin, message)
            }
            if (topology == "Full" || topology == "Line") {
              var origin: String = Random.nextInt(numNodes.toInt).toString.concat(".").concat("0")
              context.actorSelection(origin) ! Gossip(origin, message)
            }

          case "PushSum" =>
            if (topology == "Grid2d" || topology == "Imp2d") {
              var origin: String = (Random.nextInt(gridsize.toInt).toString.concat(".")).concat(Random.nextInt(gridsize.toInt).toString)
              context.actorSelection(origin) ! PushSum(0,0)
            }
            if (topology == "Full" || topology == "Line") {
              var origin: String = Random.nextInt(numNodes.toInt).toString.concat(".").concat("0")

              context.actorSelection(origin) ! PushSum(0,0)
            }

        }

      case GossipDone(source) =>


     
        
	if (!(Workerlist contains source)) {
                Workerlist += source
	}
        
	
	if ((Workerlist.length*100/numNodes) >= Failurepercent  && killit == "true") { println ("Reached convergence percent: " + Failurepercent) 
	    if (topology == "Grid2d" || topology == "Imp2d") {
	       killNode = (Random.nextInt(gridsize.toInt).toString.concat(".")).concat(Random.nextInt(gridsize.toInt).toString) }
            if (topology == "Full" || topology == "Line") {
              killNode = Random.nextInt(numNodes.toInt).toString.concat(".").concat("0") }
            println("Simulating node failure on node: " + killNode)
	    killit = "false"
	    context.actorSelection(source) ! FailNode

	    }
	    

        if (Workerlist.length == numNodes) {
	
          var time_taken = System.currentTimeMillis() - startTime

          println("Convergence time: " + time_taken.millis)
	  System.exit(0);

        }

      case PushSumDone(x: Double) =>
        var time_taken = System.currentTimeMillis() - startTime
        println("Convergence time: " + time_taken.millis)
         System.exit(0);

      

     
    }
  }

  class Worker(a: Int, b: Int, algorithm: String, topology: String, numNodes: Int) extends Actor {

    var Converged: Boolean = false
    var msgsRecvd: Int = 0
    val source = (a.toString.concat(".")).concat(b.toString)
    var message: String = "NULL"
    var msgCount: Int = 0
    var w: Double = 1
    var pushCount: Int = 0
    var neighbourList = new ArrayBuffer[String]
    var nextNode: String = "NULL"
    val gridsize = math.sqrt(numNodes)
    var s: Double = 1+ a + (gridsize*b)   
    var target: String = "NULL"
    var killSum: String = "true"
    def getNeighbours() {

      topology match {
        case "Line" =>
          if (a > 0) neighbourList += ((a - 1).toString.concat(".")).concat(b.toString)
          if (a < numNodes-1) neighbourList += ((a + 1).toString.concat(".")).concat(b.toString)
          
        case "Grid2d" =>
          if (a > 0) neighbourList += ((a - 1).toString.concat(".")).concat(b.toString)
          if (a < gridsize-1) neighbourList += ((a + 1).toString.concat(".")).concat(b.toString)
          if (b > 0) neighbourList += (a.toString.concat(".")).concat((b - 1).toString)
          if (b < gridsize-1) neighbourList += (a.toString.concat(".")).concat((b + 1).toString)
        case "Imp2d" =>
          if (a > 0) neighbourList += ((a - 1).toString.concat(".")).concat(b.toString)
          if (a < gridsize-1) neighbourList += ((a + 1).toString.concat(".")).concat(b.toString)
          if (b > 0) neighbourList += (a.toString.concat(".")).concat((b - 1).toString)
          if (b < gridsize-1) neighbourList += (a.toString.concat(".")).concat((b + 1).toString)
          neighbourList += ((Random.nextInt(gridsize.toInt).toString.concat(".")).concat(Random.nextInt(gridsize.toInt).toString))	
	case "Full" =>
	neighbourList += ((Random.nextInt(numNodes.toInt).toString.concat(".")).concat("0"))
      }
    }
    getNeighbours()
    def getNext(): String = {

      topology match {
        case "Line" =>
          var Nodeselect: Double = Math.random()
          nextNode = if (Nodeselect > 0.5 && neighbourList.length > 1) neighbourList(1) else neighbourList(0)
        case "Grid2d" =>
          nextNode = neighbourList(Random.nextInt(neighbourList.length))
        case "Imp2d" =>
          nextNode = neighbourList(Random.nextInt(neighbourList.length))
	case "Full" =>
	nextNode = ((Random.nextInt(numNodes.toInt).toString.concat(".")).concat("0"))
      }
      return nextNode
    }

    def receive = {

      case FailNode =>
       println ("This is a failed Node")    
       self ! PoisonPill

      case Gossip(origin, message) =>
        if (Converged == false) {
          msgsRecvd += 1
         
          this.message = message
          if (msgsRecvd <= END_GOSSIP-1) {
            for (i <- 0 until numofMessages) {
              var target: String = getNext()
              context.actorSelection("../" + target) ! Gossip(source, message)
                         }
          } else {
              
	      for (i <- 0 until numofMessages) {
	      
	      if (topology == "Full") target = Random.nextInt(numNodes.toInt).toString.concat(".").concat("0")
	      else target = getNext()     
              
              context.actorSelection("../" + target) ! Gossip(source, message)
                         }
	    
            context.parent ! GossipDone(source)
            Converged = true
	 
            
          }
        }
      case PushSum(s, w) =>

        msgCount = msgCount + 1
        if (Math.abs((this.s / this.w) - ((this.s + s) / (this.w + w))) <= 1E-10)
          pushCount += 1
        else
          pushCount = 0

        if (pushCount == END_PUSHSUM - 1) {
          Converged = true
          context.parent ! PushSumDone(this.s / this.w)
        }

        this.s = this.s + s
        this.w = this.w + w
        this.s = this.s / 2
        this.w = this.w / 2
	var target: String = getNext()

	if ( Random.nextInt(numNodes.toInt) == a && killSum == "true") { 
	killSum="false"
         context.actorSelection("../" + target) ! PushSum((1+ a + (gridsize*b)),1) }

    context.actorSelection("../" + target) ! PushSum(this.s, this.w)

    }

  }

}
  
