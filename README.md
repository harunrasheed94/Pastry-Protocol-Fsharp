# Pastry-Protocol-in-Fsharp
Pastry protocol implementation using Akka .net actor model in F# 
## Steps to run the code in windows:
1) Open command prompt and go to the directory where the source file is stored. 
2) Use the following command to run the source code :  
   **2.1) dotnet fsi --langversion:preview pastry.fsx 1000 10**  
    <!--- pastry.fsx is the file name, 1000 is the number of actors to be created and 10 is the number of requests to performed by each actor.  
           Note: Make sure .NET core SDK is installed to run the code. -->
