#include <ssg.h>
#include <mofka/Client.hpp>
#include <mofka/TopicHandle.hpp>
#include <tclap/CmdLine.h>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <time.h>
#include <string>
#include <iostream>
#include <chrono>
#include <fstream>

typedef std::chrono::high_resolution_clock Clock;

namespace tl = thallium;

static std::string g_ssgfile;
static std::string g_protocol;
static size_t g_events;
static std::string g_log_level = "info";
static std::string g_benchmark = "bench.out";

std::ofstream outfile;

static void parse_command_line(int argc, char** argv);

int main(int argc, char** argv) {
    parse_command_line(argc, argv);
    spdlog::set_level(spdlog::level::from_str(g_log_level));

    tl::engine engine(g_protocol, THALLIUM_SERVER_MODE);
    ssg_init();

    // open the benchmark file
    outfile.open(g_benchmark, std::ios_base::app);

    try {

        // -- Initialize a Client
        mofka::Client client(engine);

        // -- Create ServiceHandle
        mofka::ServiceHandle service = client.connect(mofka::SSGFileName{g_ssgfile});

        // -- Open a topic
        mofka::TopicHandle topic = service.openTopic("mytopic");

        // -- Create a DataSelector for the consumer
        // This example will only select events with an even id field and a value field lower than 70
        // Note: a selector returns a DataDescriptor object that must be built from the DataDescriptor
        // passed as argument. The descriptor argument can be seen as a key to access the underlying data.
        // Returning it tells Mofka "I want all the data from this descriptor". But DataDescriptor has
        // operations that can be used to tell Mofka we are interested in only a subset (e.g. a strided
        // pattern over the data, or a sub-region, like here with makeSubView).
        // mofka::DataDescriptor::Null() is our way to say we are not interested in this event's data.
        mofka::DataSelector selector = [](const mofka::Metadata& metadata, const mofka::DataDescriptor& descriptor) {
            return descriptor; // we want the full data
        };

        // -- Create a DataBroker for the consumer
        mofka::DataBroker broker = [](const mofka::Metadata& metadata, const mofka::DataDescriptor& descriptor) {
            (void)metadata;
            return mofka::Data{new char[descriptor.size()], descriptor.size()};
        };

        // -- Get a consumer for the topic
        mofka::BatchSize   batchSize   = mofka::BatchSize::Adaptive();
        mofka::ThreadCount threadCount = mofka::ThreadCount{1};
        mofka::Consumer consumer = topic.consumer("myconsumer", batchSize, threadCount, selector, broker);

        // -- Consume events
        for(size_t i=0; i < g_events; ++i) {
            auto consume_start = Clock::now();
            auto event = consumer.pull().wait();
            auto data = event.data();
            std::string_view data_str{nullptr, 0};
            if(data.size() != 0)
                data_str = std::string_view{
                    reinterpret_cast<const char*>(data.segments()[0].ptr),
                    data.segments()[0].size
                };
            spdlog::info("Received event {} with metadata {} and data {}",
                         event.id(), event.metadata().string(), data_str);
            if(i % 10 == 0) event.acknowledge();
            if(data.size()) delete[] reinterpret_cast<const char*>(data.segments()[0].ptr);
            auto consume_end = Clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(consume_end - consume_start).count();
            outfile << "mofka," << event.id() << ",consume," << data_str.size() << "," << duration << std::endl;
            spdlog::info("Consuming event took {} nanoseconds.", duration);
        }

    } catch(const mofka::Exception& ex) {
        spdlog::critical("{}", ex.what());
        exit(-1);
    }

    spdlog::info("Done!");

    ssg_finalize();
    engine.finalize();

    return 0;
}

static void parse_command_line(int argc, char** argv) {
    try {
        TCLAP::CmdLine cmd("Mofka client", ' ', "0.1");
        TCLAP::ValueArg<std::string> ssgFileArg(
                "s", "ssgfile", "SSG file of the service", true, "", "string");
        TCLAP::ValueArg<std::string> protocolArg(
                "p", "protocol", "Protocol", true, "na+sm", "string");
        TCLAP::ValuesConstraint<std::string> allowedLogLevels({
            "trace", "debug", "info", "warning", "error", "critical", "off"
        });
        TCLAP::ValueArg<size_t> events(
            "e", "events", "Number of events to read", true, 8, "int"
        );
        TCLAP::ValueArg<std::string> logLevel(
                "v", "verbose", "Logging level",
                false, "info", &allowedLogLevels);
        TCLAP::ValueArg<std::string> benchmark(
            "f", "benchmark_file", "File to write benchmarks to", false, "bench.out", "string"
        );
        cmd.add(ssgFileArg);
        cmd.add(protocolArg);
        cmd.add(events);
        cmd.add(logLevel);
        cmd.add(benchmark);
        cmd.parse(argc, argv);
        g_ssgfile = ssgFileArg.getValue();
        g_protocol = protocolArg.getValue();
        g_events = events.getValue();
        g_log_level = logLevel.getValue();
        g_benchmark = benchmark.getValue();

    } catch(TCLAP::ArgException &e) {
        std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl;
        exit(-1);
    }
}