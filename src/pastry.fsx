#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open System.Threading
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Collections.Generic

let system = ActorSystem.Create("System")

type Pastry =
    | SetUpNetwork of IActorRef
//nodeId, number of Requests, nodeMap<nodeId,actorRef>, nodeIds, routingTable (2D array 8x4), leafNodes, neighboring Nodes 
    | SetActorNode of string * int32 * Dictionary<string,IActorRef> * string[] * string[,] * string[] * string[]
    | Route of string * int32 * string * IActorRef
    | BeginRouting
    | PastryConvergence of int

//pastry nodes where routing happens
let ActorNode nodes (mailbox:Actor<_>) = 
      let mutable srcNodeId = ""
      let mutable req = 0
      let mutable hopCnt = 0
      let mutable nodeDict = new Dictionary<string, IActorRef>()
      let mutable nodeIds = Array.create nodes " "
      
      let mutable routingTable = Array2D.create 8 6 "-1"
      let mutable neighborNodes = Array.create 8 " "
      let mutable leafNodes = Array.create 8 " "

      let rec loop () = actor {
          let! msg = mailbox.Receive()
          match msg with
          //initialize the actor
          | SetActorNode (nId,nReq,nDict,nIds,rTable,lNodes,nNodes) ->
             srcNodeId <- nId
             req <- nReq
             nodeDict <- nDict
             nodeIds <- nIds 
             routingTable <- rTable 
             leafNodes <- lNodes 
             neighborNodes <- nNodes 

            //Routing   
          | Route (destNodeId,hopsPassed,msg,managerRef) ->
              
             let destNum = destNodeId |> int
             let mutable forwardNodeId = srcNodeId
             hopCnt <- hopsPassed

             //destination reached, so converge
             if srcNodeId.Equals(destNodeId) then
                managerRef <! PastryConvergence(hopCnt)
             else //try closest node among leaf nodes or routing table or both and neighbornodes(rare case)

 //if destination node is between the smallest and largest of leaf nodes, then try finding the closest node among leaf nodes 
               if destNum >= int(leafNodes.[0]) && destNum <= int(leafNodes.[7]) then
                  let mutable closestDist = destNum - int(leafNodes.[0]) |> abs
                  forwardNodeId <- leafNodes.[0]
                  for i = 1 to 7 do
                    let curDist = destNum - int(leafNodes.[i]) |> abs
                    if curDist < closestDist then
                        closestDist <- curDist
                        forwardNodeId <- leafNodes.[i]
               //end of finding closest node from leaf nodes 

 //else Use Routing Table to find the closest node to destination node and if Routing Table also fails then rare case
               else
                  //Routing Table  
                  let mutable prefixMatchDestNode = 0
                  let mutable row = 0
                  let mutable col = 0

                  for i in 0..7 do
                    if srcNodeId.[0..i] = destNodeId.[0..i] then
                         prefixMatchDestNode <- prefixMatchDestNode + 1
                  
                  row <- prefixMatchDestNode
                  col <- int destNodeId.[prefixMatchDestNode] - int '0'

                  if routingTable.[row,col] <> "-1" && prefixMatchDestNode > 0 then
                    forwardNodeId <- routingTable.[row,col]
     //routing table failed so try rare case with all neighboring, leaf and routing table nodes

    //find the node with same common prefix length as current node id but is numerically closer to destination 
                  else
                    let allNodesList = new List<string>()
                    allNodesList.InsertRange(0,leafNodes) //add Leaf Nodes
                    allNodesList.InsertRange(leafNodes.Length,neighborNodes) //add Neighbor Nodes

                    //add Routing Table Nodes
                    for r in 0..7 do
                        for c in 0..5 do
                            if routingTable.[r,c] <> "-1" then
                                allNodesList.Add(routingTable.[r,c])
                    
                    let allNodesSet = Set.ofSeq allNodesList
                    forwardNodeId <- srcNodeId

                    for nextNode in allNodesSet do
                      let mutable newPrefixMatch = 0 
                      for i in 0..7 do
                         if nextNode.[0..i] = destNodeId.[0..i] then
                            newPrefixMatch <- newPrefixMatch + 1 
                         
                         if newPrefixMatch > prefixMatchDestNode then
                            prefixMatchDestNode <- newPrefixMatch
                            forwardNodeId <- nextNode
                         elif newPrefixMatch = prefixMatchDestNode then 
                            let nextNodeDist =  destNum - int(nextNode) |> abs
                            let distFromSource = destNum - int(srcNodeId) |> abs
                            if nextNodeDist < distFromSource then
                                prefixMatchDestNode <- newPrefixMatch
                                forwardNodeId <- nextNode
                  //end of rare case scenario
               //end of Routing Table and rare case scenario         
             //end of finding closest node (next node to be forwarded to)
                 
              //if no next node closer than source node, destination reached so converge    
               if forwardNodeId.Equals(srcNodeId) then
                managerRef <! PastryConvergence(hopCnt)
               else //Forward to next node
                let nodeRef:IActorRef = snd (nodeDict.TryGetValue forwardNodeId)
                nodeRef <! Route(destNodeId,hopCnt+1,"Pastry",managerRef)
     
          | _-> () 
          return! loop()
      }
      loop ()

//Manager Function to set up network for all the child actors here and start the routing
let Manager nodes req (mailbox:Actor<_>) = 

    let mutable totalHopCnt = 0
    let mutable totalRequestsMade = 0
    let mutable managerRef:IActorRef = null
    let mutable nodeIds = Array.create nodes " "
    let mutable sortedNodeIds = Array.create nodes " "
    let nodeDict = new Dictionary<string, IActorRef>() 

    let rec loop () = actor {
        let! msg = mailbox.Receive()
        match msg with
        | SetUpNetwork (mId) ->
             managerRef <- mId
             let rnd  = System.Random()
             
             //spawn multiple actors and assign unique node Ids of length 8 for each actor
            // printfn "All Node Ids "
             for i = 0 to nodes-1 do
                let actorRef = ActorNode nodes |> spawn system ("ActorNode"+string(i+1))
                let mutable nodeIdGenerated = false
                
                while not nodeIdGenerated do
                    let mutable idx = 0
                    let mutable nodeId = ""

                    while idx <> 8 do
                        nodeId <- nodeId + string(rnd.Next(0,6)) //each digit in nodeID will have only digits 0 to 3
                        idx <- idx + 1
                    
                    if not(nodeDict.ContainsKey(nodeId)) then
                        nodeDict.Add(nodeId,actorRef)
                        nodeIds.[i] <- nodeId
                        nodeIdGenerated <- true
              
             sortedNodeIds <- Array.sort nodeIds
             printfn "Node Ids Generated" 
             //Set neighbor Nodes, leafnodes and routing table for each node
             for i=0 to nodes-1 do
                let mutable leafNodes = Array.create 8 " "
                let mutable neighborNodes = Array.create 8 " "
                let mutable routingTable = Array2D.create 8 6 "-1"
                let nodeId = nodeIds.[i]
                //printfn "Node Id = %A" nodeId

                //set 8 Neighbor nodes
                for i = 0 to 7 do
                   neighborNodes.[i] <- nodeIds.[(i+1)%nodes]
                //neighbor nodes assigned

                //set 8 Leaf nodes by using sorted array of nodeIds (sortedNodeIds)  
                let sortedIdx = Array.findIndex (fun e->e = nodeId) sortedNodeIds
                let mutable startIdx = 0

                if ((sortedIdx + 4) > nodes-1) then //if nodeId is among last 4 nodeIds (MORE SMALLER LEAF NODES)
                    startIdx <- nodes - 9           //then it will have the last 8 nodes as the leaf nodes
                elif (sortedIdx - 4) < 0 then    //if nodeId is among first 4 nodeIDs (MORE LARGER LEAF NODES)
                    startIdx <- 0                //then it will have first 8 nodes as leaf nodes
                else                              //else 4 SMALLER LEAF NODES and 4 LARGER LEAF NODES 
                    startIdx <- sortedIdx - 4    //(EQUAL Number of SMALLER and LARGER LEAF NODES)
               // printfn "Node Id = %s" nodeId    

                for i = 0 to 7 do
                   if startIdx = sortedIdx then
                      startIdx <- startIdx + 1
                   
                   leafNodes.[i] <- sortedNodeIds.[startIdx]
                   startIdx <- startIdx + 1
                //Leaf nodes assigned

                //Routing Table Construction 8 x 6 2D array begin
                let mutable isFilled = Array2D.create 8 6 false
                for entry in nodeDict do 
                    let mutable row = 0
                    let mutable col = 0
                    let mutable flag = false
                    let randomNodeId = entry.Key
                    if randomNodeId <> nodeId then
                        while not flag do 
                            if randomNodeId.[row] = nodeId.[row] then //find longest common prefix
                                row <- row + 1
                            else  //add the randomNodeId to the routing table based on the prefix found
                                col <- int randomNodeId.[row] - int '0'
                                let filled = isFilled.[row,col]
                                if not filled then
                                    routingTable.[row,col] <- randomNodeId
                                    flag <- true
                                    isFilled.[row,col] <- true
                                else
                                    flag <- true
                                    
                //Routing Table Construction finished 8 x 4 2D array ended
               // printfn "Node %s" nodeId
              //  printfn "%A" routingTable

                //Set the values in the respective actor begin
                let nodeKey, nodeRef = nodeDict.TryGetValue nodeId
                nodeRef <!  SetActorNode(nodeId,req,nodeDict,nodeIds,routingTable,leafNodes,neighborNodes)
                //Set the values in the respective actor ended
             printfn "Network set"
             managerRef <! BeginRouting

        | BeginRouting ->
             
             printfn "Starting Pastry"
             for i = 0 to nodes-1 do
                let mutable destNode = i
                let nodeId = nodeIds.[i]
                let mutable reqCounter = req
                let nodeRef:IActorRef = snd (nodeDict.TryGetValue nodeId)
                
                while reqCounter > 0 do
                    destNode <- (destNode+1)%nodes
                    nodeRef <! Route(nodeIds.[destNode],0,"Pastry",managerRef)
                    reqCounter <- reqCounter - 1

        | PastryConvergence (hopCnt) ->
             
             totalHopCnt <- totalHopCnt + hopCnt
             totalRequestsMade <- totalRequestsMade + 1

             if totalRequestsMade = nodes * req  then
                Thread.Sleep(3000)
                printfn "Total Requests Made = %d"totalRequestsMade
                let avgHopCnt = float(totalHopCnt)/float(totalRequestsMade)
                printfn "Average Hop Count: %f" avgHopCnt
                Environment.Exit 0

        | _-> () 
        return! loop()
    }
    loop()

//Read the inputs      
let args : string array = fsi.CommandLineArgs |> Array.tail
let n = args.[0] |> int
let req = args.[1] |> int

//spawn the manager 
let managerRef = Manager n req |> spawn system "Manager"
managerRef <! SetUpNetwork(managerRef)
System.Console.ReadLine() |> ignore 