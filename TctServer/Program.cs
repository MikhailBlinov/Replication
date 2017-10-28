using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ReplicationProject;

namespace TctServer
{
    class Program
    {
        static void Main(string[] args)
        {
            using (Socket serverSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
            {
                Console.WriteLine(" The server is waiting incoming messages ........ ");

                IPAddress address = IPAddress.Parse("127.0.0.1");

                EndPoint endPoint = new IPEndPoint(address, 9600);

                serverSocket.Bind(endPoint);

                serverSocket.Listen(200);

                int threadCount = 1;

                while (true)
                {
                    Socket clientSocket = serverSocket.Accept();

                    Thread thread = new Thread(ThreadFunction);
                    thread.Name = "Thread number  = " + threadCount++;
                    thread.Start(clientSocket);
                }
            }
        }

        static void ThreadFunction(object obj)
        {
            Socket clientSocket = (Socket) obj;

            try
            {
                int count = 0;

                byte[] numberBytes = new byte[2];
                count = clientSocket.Receive(numberBytes, 0, 2, SocketFlags.None);

                int orderNumber = BitConverter.ToInt16(numberBytes,0);

                Console.WriteLine(" OrderNumber =  {0}", orderNumber);

                byte[] countBytes = new byte[4];
                count = clientSocket.Receive(countBytes, 0, 4, SocketFlags.None);

                int bytesNumber = BitConverter.ToInt16(countBytes, 0);

                byte[] resultBytes = new byte[8192];
                count = clientSocket.Receive(resultBytes, 0, bytesNumber, SocketFlags.None);

                string connectionString = @" Data Source = .\sqlexpress; Initial Catalog = TestDataBase; Integrated Security = true;";

                Destination destination = new Destination(connectionString);

                destination.PerformReplication(resultBytes);

                Console.WriteLine("From the project " + Thread.CurrentThread.Name);
            }
            finally
            {
              clientSocket.Dispose();    
            }

            Console.WriteLine(Environment.NewLine);
        }
    }
}
