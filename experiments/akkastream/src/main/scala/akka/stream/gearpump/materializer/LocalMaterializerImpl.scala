/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package akka.stream.gearpump.materializer

import java.util.concurrent.atomic.AtomicBoolean
import java.{util => ju}

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.Dispatchers
import akka.stream.gearpump.module.ReduceModule
import akka.stream.impl.Stages.{DirectProcessor, StageModule}
import akka.stream.impl.StreamLayout.{AtomicModule, Module}
import akka.stream.impl._
import akka.stream.impl.fusing.GraphInterpreter.GraphAssembly
import akka.stream.impl.fusing.{Map => MMap, _}
import akka.stream.impl.io.{TLSActor, TlsModule}
import akka.stream.{ActorMaterializerSettings, Attributes, InPort, MaterializationContext, OutPort, Graph => AkkaGraph}
import org.reactivestreams.{Processor, Publisher, Subscriber}

/**
 * This materializer is functional equivalent to [[akka.stream.impl.ActorMaterializerImpl]]
 *
 * @param system
 * @param settings
 * @param dispatchers
 * @param supervisor
 * @param haveShutDown
 * @param flowNames
 */
class LocalMaterializerImpl (
    system: ActorSystem,
    settings: ActorMaterializerSettings,
    dispatchers: Dispatchers,
    supervisor: ActorRef,
    haveShutDown: AtomicBoolean,
    flowNames: SeqActorName)
  extends LocalMaterializer(
    system, settings, dispatchers, supervisor,
    haveShutDown, flowNames){

  case class LocalMaterializerSession(module: Module, iAttributes: Attributes, subflowFuser: GraphInterpreterShell ⇒ ActorRef = null) extends MaterializerSession(module, iAttributes) {

    override protected def materializeAtomic(atomic: AtomicModule, effectiveAttributes: Attributes, matVal: ju.Map[Module, Any]): Unit = {

      def newMaterializationContext() =
        new MaterializationContext(LocalMaterializerImpl.this, effectiveAttributes, stageName(effectiveAttributes))
      atomic match {
        case sink: SinkModule[_, _] ⇒
          val (sub, mat) = sink.create(newMaterializationContext())
          assignPort(sink.shape.in, sub.asInstanceOf[Subscriber[Any]])
          matVal.put(atomic, mat)
        case source: SourceModule[_, _] ⇒
          val (pub, mat) = source.create(newMaterializationContext())
          assignPort(source.shape.out, pub.asInstanceOf[Publisher[Any]])
          matVal.put(atomic, mat)

        // FIXME: Remove this, only stream-of-stream ops need it
        case stage: StageModule ⇒
          val (processor, mat) = processorFor(stage, effectiveAttributes, effectiveSettings(effectiveAttributes))
          assignPort(stage.inPort, processor)
          assignPort(stage.outPort, processor)
          matVal.put(atomic, mat)

        case tls: TlsModule ⇒ // TODO solve this so TlsModule doesn't need special treatment here
          val es = effectiveSettings(effectiveAttributes)
          val props =
            TLSActor.props(es, tls.sslContext, tls.firstSession, tls.role, tls.closing, tls.hostInfo)
          val impl = actorOf(props, stageName(effectiveAttributes), es.dispatcher)
          def factory(id: Int) = new ActorPublisher[Any](impl) {
            override val wakeUpMsg = FanOut.SubstreamSubscribePending(id)
          }
          val publishers = Vector.tabulate(2)(factory)
          impl ! FanOut.ExposedPublishers(publishers)

          assignPort(tls.plainOut, publishers(TLSActor.UserOut))
          assignPort(tls.cipherOut, publishers(TLSActor.TransportOut))

          assignPort(tls.plainIn, FanIn.SubInput[Any](impl, TLSActor.UserIn))
          assignPort(tls.cipherIn, FanIn.SubInput[Any](impl, TLSActor.TransportIn))

          matVal.put(atomic, NotUsed)

        case graph: GraphModule ⇒
          matGraph(graph, effectiveAttributes, matVal)

        case stage: GraphStageModule ⇒
          val graph =
            GraphModule(GraphAssembly(stage.shape.inlets, stage.shape.outlets, stage.stage),
              stage.shape, stage.attributes, Array(stage))
          matGraph(graph, effectiveAttributes, matVal)
      }
    }

    private def matGraph(graph: GraphModule, effectiveAttributes: Attributes, matVal: ju.Map[Module, Any]): Unit = {
      val calculatedSettings = effectiveSettings(effectiveAttributes)
      val (inHandlers, outHandlers, logics) = graph.assembly.materialize(effectiveAttributes, graph.matValIDs, matVal, registerSrc)

      val shell = new GraphInterpreterShell(graph.assembly, inHandlers, outHandlers, logics, graph.shape,
        calculatedSettings, LocalMaterializerImpl.this)

      val impl =
        if (subflowFuser != null && !effectiveAttributes.contains(Attributes.AsyncBoundary)) {
          subflowFuser(shell)
        } else {
          val props = ActorGraphInterpreter.props(shell)
          actorOf(props, stageName(effectiveAttributes), calculatedSettings.dispatcher)
        }

      for ((inlet, i) ← graph.shape.inlets.iterator.zipWithIndex) {
        val subscriber = new ActorGraphInterpreter.BoundarySubscriber(impl, shell, i)
        assignPort(inlet, subscriber)
      }
      for ((outlet, i) ← graph.shape.outlets.iterator.zipWithIndex) {
        val publisher = new ActorGraphInterpreter.BoundaryPublisher(impl, shell, i)
        impl ! ActorGraphInterpreter.ExposedPublisher(shell, i, publisher)
        assignPort(outlet, publisher)
      }
    }

    // FIXME: Remove this, only stream-of-stream ops need it
    private def processorFor(op: StageModule,
                             effectiveAttributes: Attributes,
                             effectiveSettings: ActorMaterializerSettings): (Processor[Any, Any], Any) = op match {
      case DirectProcessor(processorFactory, _) ⇒ processorFactory()
      case _ ⇒
        val (opprops, mat) = ActorProcessorFactory.props(LocalMaterializerImpl.this, op, effectiveAttributes)
        ActorProcessorFactory[Any, Any](
          actorOf(opprops, stageName(effectiveAttributes), effectiveSettings.dispatcher)) -> mat
    }

  }

  private[this] def createFlowName(): String = flowNames.next()

  val flowName = createFlowName()
  var nextId = 0

  def stageName(attr: Attributes): String = {
    val name = s"$flowName-$nextId-${attr.nameOrDefault()}"
    nextId += 1
    name
  }

}

object LocalMaterializerImpl {
  case class MaterializedModule(val module: Module, val matValue: Any, inputs: Map[InPort, Subscriber[_]] = Map.empty[InPort, Subscriber[_]] , outputs: Map[OutPort, Publisher[_]] = Map.empty[OutPort, Publisher[_]])

  def toFoldModule(reduce: ReduceModule[Any]): Fold[Any,Any] = {
    val f = reduce.f
    val aggregator = {(zero: Any, input: Any) =>
      if (zero == null) {
        input
      } else {
        f(zero, input)
      }
    }
    Fold(null, aggregator, null)
  }
}