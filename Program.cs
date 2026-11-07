using System;
using System.Collections.Generic;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;
using System.IO;

namespace MT4WebSocketServer
{
    class Program
    {
        private static List<WebSocket> _clients = new List<WebSocket>();
        private static Dictionary<WebSocket, string> _clientAccounts = new Dictionary<WebSocket, string>();
        private static HttpListener _httpListener;

        static async Task Main(string[] args)
        {
            Console.Title = "MT4 WebSocket & Webhook Server";
            Console.WriteLine("==============================================");
            Console.WriteLine("   MT4 WebSocket Server + TradingView Webhook");
            Console.WriteLine("   WebSocket: ws://localhost:8080");
            Console.WriteLine("   Webhook: http://localhost:8081/webhook");
            Console.WriteLine("==============================================");
            Console.WriteLine("Press Ctrl+C to stop the server");
            Console.WriteLine("==============================================");

            // WebSocket server
            var wsTask = RunWebSocketServer("http://+:8080/");

            // Webhook server
            var httpTask = RunWebhookServer("http://+:8081/");

            await Task.WhenAll(wsTask, httpTask);
        }

        private static async Task RunWebSocketServer(string prefix)
        {
            _httpListener = new HttpListener();
            _httpListener.Prefixes.Add(prefix);
            _httpListener.Start();

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, e) =>
            {
                Console.WriteLine("\nShutting down server...");
                cts.Cancel();
                _httpListener.Stop();
                Environment.Exit(0);
            };

            while (!cts.IsCancellationRequested)
            {
                try
                {
                    var context = await _httpListener.GetContextAsync();
                    if (context.Request.IsWebSocketRequest)
                    {
                        _ = Task.Run(() => ProcessWebSocketRequest(context, cts.Token));
                    }
                    else
                    {
                        context.Response.StatusCode = 400;
                        context.Response.Close();
                    }
                }
                catch (Exception ex)
                {
                    if (_httpListener.IsListening)
                        Console.WriteLine($"WebSocket Error: {ex.Message}");
                    break;
                }
            }
        }

        private static async Task RunWebhookServer(string prefix)
        {
            var listener = new HttpListener();
            listener.Prefixes.Add(prefix);
            listener.Start();

            Console.WriteLine($"Webhook server listening on {prefix}");

            while (true)
            {
                try
                {
                    var context = await listener.GetContextAsync();
                    _ = Task.Run(() => ProcessWebhookRequest(context));
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Webhook Error: {ex.Message}");
                    break;
                }
            }
        }

        private static async Task ProcessWebhookRequest(HttpListenerContext context)
        {
            HttpListenerRequest request = context.Request;
            HttpListenerResponse response = context.Response;

            try
            {
                // Only process POST requests
                if (request.HttpMethod != "POST")
                {
                    response.StatusCode = 405;
                    response.Close();
                    return;
                }

                // Read the request body
                string body;
                using (var reader = new StreamReader(request.InputStream, request.ContentEncoding))
                {
                    body = await reader.ReadToEndAsync();
                }

                // Console.WriteLine($"📨 Webhook received: {body}");
                await BroadcastMessage(body, CancellationToken.None);

                // Send response
                response.StatusCode = 200;
                byte[] buffer = Encoding.UTF8.GetBytes("{\"status\": \"success\", \"message\": \"Alert sent to MT4\"}");
                response.ContentLength64 = buffer.Length;
                response.OutputStream.Write(buffer, 0, buffer.Length);
                response.Close();

                Console.WriteLine($"✅ Webhook to MT4: {body}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Webhook error: {ex.Message}");

                // Send error response
                response.StatusCode = 500;
                byte[] buffer = Encoding.UTF8.GetBytes($"{{\"status\": \"error\", \"message\": \"{ex.Message}\"}}");
                response.ContentLength64 = buffer.Length;
                response.OutputStream.Write(buffer, 0, buffer.Length);
                response.Close();
            }
        }

        private static async Task ProcessWebSocketRequest(HttpListenerContext context, CancellationToken cancellationToken)
        {
            WebSocket webSocket = null;
            try
            {
                var webSocketContext = await context.AcceptWebSocketAsync(null);
                webSocket = webSocketContext.WebSocket;
                
                lock (_clients)
                {
                    _clients.Add(webSocket);
                }

                Console.WriteLine($"✅ MT4 Client connected. Total clients: {_clients.Count}");

                await HandleWebSocketConnection(webSocket, cancellationToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"WebSocket error: {ex.Message}");
            }
            finally
            {
                if (webSocket != null)
                {
                    lock (_clients)
                    {
                        _clients.Remove(webSocket);
                    }
                    if (_clientAccounts.ContainsKey(webSocket))
                    {
                        _clientAccounts.Remove(webSocket);
                    }
                    Console.WriteLine($"❌ MT4 Client disconnected. Total clients: {_clients.Count}");
                    webSocket.Dispose();
                }
            }
        }

        private static async Task HandleWebSocketConnection(WebSocket webSocket, CancellationToken cancellationToken)
        {
            var buffer = new byte[4096];

            try
            {
                while (webSocket.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
                {
                    var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), cancellationToken);
                    
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Client closed", cancellationToken);
                        break;
                    }

                    string message = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    Console.WriteLine($"📨 Received from MT4: {message}");

                    // Handle account registration
                    if (message.StartsWith("account:"))
                    {
                        var account = message.Substring(8).Trim();
                        _clientAccounts[webSocket] = account;
                        Console.WriteLine($"💰 Account registered: {account}");
                        
                        // Send confirmation
                        await SendMessage(webSocket, $"account_registered:{account}", cancellationToken);
                    }
                    else if (message == "ping")
                    {
                        await SendMessage(webSocket, "pong", cancellationToken);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"WebSocket connection error: {ex.Message}");
            }
        }

        private static async Task SendMessage(WebSocket webSocket, string message, CancellationToken cancellationToken)
        {
            if (webSocket.State == WebSocketState.Open)
            {
                byte[] buffer = Encoding.UTF8.GetBytes(message);
                await webSocket.SendAsync(new ArraySegment<byte>(buffer), 
                                        WebSocketMessageType.Text, 
                                        true, 
                                        cancellationToken);
            }
        }

        // Broadcast to all connected MT4 clients
        public static async Task BroadcastMessage(string message, CancellationToken cancellationToken)
        {
            if (_clients.Count == 0)
            {
                Console.WriteLine("⚠️  No MT4 clients connected. Message not delivered.");
                return;
            }

            byte[] buffer = Encoding.UTF8.GetBytes(message);
            List<WebSocket> clientsCopy;
            
            lock (_clients)
            {
                clientsCopy = new List<WebSocket>(_clients);
            }

            foreach (var client in clientsCopy)
            {
                if (client.State == WebSocketState.Open)
                {
                    try
                    {
                        await client.SendAsync(new ArraySegment<byte>(buffer), 
                                             WebSocketMessageType.Text, 
                                             true, 
                                             cancellationToken);
                        // Console.WriteLine($"✅ Alert sent to MT4: {message}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"❌ Failed to send to MT4 client: {ex.Message}");
                    }
                }
            }
        }
    }
}