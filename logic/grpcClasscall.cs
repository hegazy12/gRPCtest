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


      
      
      
        public override async Task<MetricResponse> StreamMetrics(IAsyncStreamReader<MetricRequest> requestStream, ServerCallContext context)
        {
            int count = 0;

            // Read incoming metrics sequentially as they arrive from the client
            await foreach (var metric in requestStream.ReadAllAsync(context.CancellationToken))
            {
                count++;
                // Process each metric item here (e.g., save to a database or time-series store)
                Console.WriteLine($"Stream item #{count}: {metric.MetricName} = {metric.Value}");
            }

            // Once the client finishes streaming, return a single summary response
            return new MetricResponse
            {
                Success = true,
                Message = $"Successfully processed {count} streamed metrics."
            };
}
}