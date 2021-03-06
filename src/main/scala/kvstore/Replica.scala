package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ask, pipe}
import scala.concurrent.duration._
import akka.util.Timeout

import scala.util.Random

object Replica {

    sealed trait Operation {
        def key: String

        def id: Long
    }

    case class Insert(key: String, value: String, id: Long) extends Operation

    case class Remove(key: String, id: Long) extends Operation

    case class Get(key: String, id: Long) extends Operation

    sealed trait OperationReply

    case class OperationAck(id: Long) extends OperationReply

    case class OperationFailed(id: Long) extends OperationReply

    case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

    def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

    import Replica._
    import Replicator._
    import Persistence._
    import context.dispatcher

    /*
     * The contents of this actor is just a suggestion, you can implement it in any way you like.
     */

    var updateOp: Map[Long, Cancellable] = Map.empty[Long, Cancellable]

    var persistOp: Map[Long, Cancellable] = Map.empty[Long, Cancellable]

    var replicateOp: Map[Long, Cancellable] = Map.empty[Long, Cancellable]

    var operations: Map[Long, (String, Option[String])] = Map.empty[Long, (String, Option[String])]

    var operationRequester: Map[Long, ActorRef] = Map.empty[Long, ActorRef]

    var expectedSeq: Long = 0L

    var seqToReplicator: Map[Long, ActorRef] = Map.empty[Long, ActorRef]

    var kv = Map.empty[String, String]
    // a map from secondary replicas to replicators
    var secondaries = Map.empty[ActorRef, ActorRef]
    // the current set of replicators
    var replicators = Set.empty[ActorRef]

    var repAckn: Map[ActorRef, Set[Long]] = Map.empty[ActorRef, Set[Long]]

    val persistence = context.system.actorOf(persistenceProps)

    override val supervisorStrategy = OneForOneStrategy()
    {
        case _: PersistenceException => SupervisorStrategy.restart
    }

    // register self in a Arbiter
    arbiter ! Join

    def receive = {
        case JoinedPrimary => context.become(leader)
        case JoinedSecondary => context.become(replica)
    }

    /* TODO Behavior for  the leader role. */
    val leader: Receive = {
        case ins@Insert(key, value, id) => opProcessing(id, key, Some(value), sender)

        case rm@Remove(key, id) => opProcessing(id, key, None, sender)

        case get@Get(key, id) => sender ! GetResult(key, kv.get(key), id)

        case Persisted(key, id) => acknowledgement(id)

        case Replicas(replicas) =>
        {

            val secondariesReplicasSet: Set[ActorRef] = secondaries.toSet[(ActorRef, ActorRef)].map(v => v._1)
            val replicasToUpdate = replicas diff secondariesReplicasSet
            val replicasToRemove = secondariesReplicasSet diff replicas
            val replicatorsToRemove = secondaries.filter(el => replicasToRemove.contains(el._1)).toSet[(ActorRef, ActorRef)].map(v => v._2)

            replicasToRemove.foreach(_ ! PoisonPill)
            replicatorsToRemove.foreach(_ ! PoisonPill)

            replicatorsToRemove.foreach(replicator => repAckn -= replicator)

            replicasToRemove.foreach(secondaries -= _)
            replicatorsToRemove.foreach(replicators -= _)

            if(secondaries.size == 0 && (replicasToUpdate - self).isEmpty)
            {
               replicateOp.foreach{ case (k, c) =>
                   c.cancel()
                   acknowledgement(k)
               }
            }
            (replicasToUpdate - self).foreach{ replica =>
                val replicator = context.system.actorOf(Replicator.props(replica)) // создаем репликаторы для новых реплик
                secondaries += replica -> replicator
                replicators += replicator
                // запускаем репликацию сущ. данных в новые реплики
                kv.foreach{ case (k, v) =>
                    val doomyId = Random.nextLong * 1000L
                    replicator ! Replicate(k, Some(v), doomyId)
                        repAckn = addToReplAckn(repAckn, replicator, doomyId)
                }
            }
        }
        case Replicated(key, id) =>
        {
            repAckn = rmFromReplAckn(repAckn, sender, id)
            println(s"replicated id: $id")
            if(repAckn.isEmpty)
            {
                println("replication and persistent finished, can acknowledge")
                replicateOp.get(id).map{ c => c.cancel(); println("replication timeout cancelled")}
                replicateOp -= id
                if(persistOp.get(id).map(_.isCancelled).getOrElse(false))
                {
                    operationRequester.get(id).map(_ ! OperationAck(id))
                    operationRequester -= id
                }
            }
        }
        case msg => println("Unknown message" + msg)
    }

    private def acknowledgement(id: Long) =
    {
        persistOp.get(id).map(_.cancel())
        updateOp.get(id).map
        {c =>
            c.cancel()
            if(repAckn.isEmpty)
            {
                operationRequester.get(id).map(_ ! OperationAck(id))
                operationRequester -= id
            }
            operations.get(id) match
            {
                case Some((key, Some(v))) => kv += key -> v
                case Some((key, None)) => kv -= key
                case None => println("Oooops, operations is empty!")
            }
        }
    }

    private def opProcessing(id: Long, key: String, value: Option[String], requester: ActorRef) =
    {
        operationRequester += id -> requester
        operations += id -> (key, value)

        replicators.foreach{ replicator =>
            replicator ! Replicate(key, value, id)
            repAckn = addToReplAckn(repAckn, replicator, id)
        }
        //шедьюлим перситенс
        val persOp = context.system.scheduler.schedule(0 millis, 100 millis){persistence ! Persist(key, value, id)}

        // таймаут на операцию
        val op = context.system.scheduler.scheduleOnce(1 second){
            requester ! OperationFailed(id)
            updateOp -= id
            persistOp -= id
            operations -= id
        }
        if(replicators.nonEmpty){
            //таймаут на репликацию
            replicateOp += id -> context.system.scheduler.scheduleOnce(1 second){println(s"replic timeot id: $id!");requester ! OperationFailed(id)}
        }
        updateOp += id -> op
        persistOp += id -> persOp
    }

    private def addToReplAckn(m: Map[ActorRef, Set[Long]], k: ActorRef, v: Long) =
    {
        m.get(k).fold(m.updated(k, Set(v)))(f => m.updated(k, f + v))
    }

    private def rmFromReplAckn(m: Map[ActorRef, Set[Long]], k: ActorRef, v: Long) =
    {
        m.get(k).fold(m)(f => if((f - v).isEmpty) m - k else m.updated(k, f - v))
    }

    /* TODO Behavior for the replica role. */
    val replica: Receive = {
        case get@Get(key, id) =>
        {
            sender ! GetResult(key, kv.get(key), id)
        }
        case Snapshot(key, valueOpt, seq) =>
        {
            if(seq < expectedSeq)
                sender ! SnapshotAck(key, seq)
            else if(seq == expectedSeq && seqToReplicator.get(seq).isEmpty)
            {
                // здесь нужно обновлять значение не дожидаясь подтверждения персистенса
                valueOpt match
                {
                    case Some(v) => kv += key -> v
                    case None => kv -= key
                }
                seqToReplicator += seq -> sender
                opProcessing(seq, key, valueOpt, self)
            }
        }
        case Persisted(key, id) => acknowledgement(id)

        case OperationAck(id) =>
        {
            operations.get(id).map{ case(k: String, v: Option[String]) => seqToReplicator.get(id).map(_ ! SnapshotAck(k, id))}
            seqToReplicator -= id
            if(expectedSeq < id +1) expectedSeq += 1
        }
        case OperationFailed(id) =>println(s"Secondary replica op: $id persistent failed!")

        case _ =>
    }

}

