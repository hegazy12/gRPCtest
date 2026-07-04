using System;
using System.Threading.Tasks;
using Grpc.Core;
using gRPCtest;

namespace gRPCtest.logic;

public class grpcClasscall : TelemetryService.TelemetryServiceBase
{
   public override Task<MetricResponse> TrackMetric(MetricRequest request, ServerCallContext context)
        {
            // Convert google.protobuf.Timestamp to .NET DateTimeOffset easily
            DateTimeOffset timestamp = request.Stamp.ToDateTimeOffset();

            Console.WriteLine($"Received Metric: {request.MetricName} = {request.Value} at {timestamp}");
            var httpContext = context.GetHttpContext();
            var userAgent = context.RequestHeaders.GetValue("user-agent");
            Console.WriteLine($"User-Agent from headers: {userAgent}");
            Console.WriteLine($"Client IP: {httpContext.Connection.RemoteIpAddress}");
            Console.WriteLine($"Client Port: {httpContext.Connection.RemotePort}");
            Console.WriteLine($"Request Path: {httpContext.Request.Path}");
            Console.WriteLine($"Request Method: {httpContext.Request.Method}");
            Console.WriteLine($"Request Protocol: {httpContext.Request.Protocol}");
            Console.WriteLine($"Request Headers: {string.Join(", ", httpContext.Request.Headers)}");
            // Access tags map
            if (request.Tags.ContainsKey("environment"))
            {
                Console.WriteLine($"Env: {request.Tags["environment"]}");
            }

            return Task.FromResult(new MetricResponse
            {
                Success = true,
                Message = $"Metric '{request.MetricName}' recorded successfully."
            });
        }


      /// <summary>
        /// 2. دالة الاستريم ثنائي الاتجاه (Bidirectional Streaming) 
        /// الكلاينت بيبعت استريم والسيرفر بيرد فوراً في نفس الوقت ومن غير ما يقفل الخط
        /// </summary>
        public override async Task StreamMetrics(
            IAsyncStreamReader<MetricRequest> requestStream, 
            IServerStreamWriter<MetricResponse> responseStream, 
            ServerCallContext context)
        {
            Console.WriteLine("Started bidirectional metric streaming session.");
            int counter = 0;

            try
            {
                // الـ Loop دي بتفضل مستمعة طول ما الكلاينت بيبعت بيانات حتة حتة
                await foreach (var request in requestStream.ReadAllAsync(context.CancellationToken))
                {
                    counter++;
                    
                    // طباعة البيانات المستلمة (تقدر تقرأ الـ Tags والـ Stamp عادي)
                    Console.WriteLine($"[Stream item #{counter}] Received: {request.MetricName} = {request.Value}");

                    // معالجة البيانات هنا...

                    // الرد الفوري على الكلاينت بدون إغلاق الاستريم
                    await responseStream.WriteAsync(new MetricResponse
                    {
                        Success = true,
                        Message = $"[Server ACK] Packet #{counter} for '{request.MetricName}' processed."
                    }, context.CancellationToken);
                }

                Console.WriteLine("Client closed the stream. Session ended successfully.");
            }
            catch (OperationCanceledException)
            {
                // دي بتحصل لو الكلاينت كنسل الاتصال فجأة (الـ Cancellation Token هيضرب هنا)
                Console.WriteLine("Streaming was canceled by the client.");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex + "An error occurred during streaming.");
                throw new RpcException(new Status(StatusCode.Internal, ex.Message));
            }
        }



      
      
//         public override async Task<MetricResponse> StreamMetrics(IAsyncStreamReader<MetricRequest> requestStream,IServerStreamWriter<MetricResponse> responseStream,  ServerCallContext context)
//         {
//             int count = 0;
            
//             var httpContext = context.GetHttpContext();
//             var userAgent = context.RequestHeaders.GetValue("user-agent");
//             Console.WriteLine($"User-Agent from headers: {userAgent}");
//             Console.WriteLine($"Client IP: {httpContext.Connection.RemoteIpAddress}");
//             Console.WriteLine($"Client Port: {httpContext.Connection.RemotePort}");
//             Console.WriteLine($"Request Path: {httpContext.Request.Path}");
//             Console.WriteLine($"Request Method: {httpContext.Request.Method}");
//             Console.WriteLine($"Request Protocol: {httpContext.Request.Protocol}");
//             Console.WriteLine($"Request Headers: {string.Join(", ", httpContext.Request.Headers)}");
//             // Read incoming metrics sequentially as they arrive from the client
//             await foreach (var metric in requestStream.ReadAllAsync(context.CancellationToken))
//             {
//                 count++;
//                 // Process each metric item here (e.g., save to a database or time-series store)
//                 Console.WriteLine($"Stream item #{count}: {metric.MetricName} = {metric.Value}");

//             await responseStream.WriteAsync(new MetricResponse 
//                  { 
//                    Message = $"{metric.MetricName} processed successfully.",
//                      Success = true
//                 }, context.CancellationToken);
//         }

//             // Once the client finishes streaming, return a single summary response
//        return new MetricResponse
//             {
//                 Success = true,
//                 Message = $"Successfully processed {count} streamed metrics."
//         };
//     }
}