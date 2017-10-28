using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using ReplicationProject;

namespace TcpClient
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            using (Socket clientSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
            {
                IPAddress address = IPAddress.Parse("127.0.0.1");
                EndPoint endPoint = new IPEndPoint(address, 9600);

                clientSocket.Connect(endPoint);

                string connectionString = @" Data Source = .\sqlexpress; Initial Catalog = TestDataBase; Integrated Security = true;";
                string sourceTable = "TestTableFrom";

                Source sourceData = new Source(connectionString, sourceTable);

                byte[] bytes = sourceData.PrepareTableData();

                byte[] sendBytes = null;

                using (MemoryStream stream = new MemoryStream())
                {
                    short number = 1;
                    byte[] numberBytes = BitConverter.GetBytes(number);
                    stream.Write(numberBytes, 0,2);

                    int length = bytes.Length;
                    byte[] lengthBytes = BitConverter.GetBytes(length);
                    stream.Write(lengthBytes, 0, 4);

                    stream.Write(bytes, 0, bytes.Length);

                    sendBytes = stream.ToArray();
                }

                clientSocket.Send(sendBytes);
            }
        }
    }
}
