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

        // -- Create a topic
        // A default configuration is currently equivalent to the one shown commented
        mofka::TopicBackendConfig config{/*R"(
        {
            "__type__": "default",
            "data_store": {
                "__type__": "memory"
            }
        }
        )"*/};
        // We provide a default validator, selector, and serializer as example for the API.
        mofka::Validator      validator;
        mofka::Serializer     serializer;
        mofka::TargetSelector selector;
        mofka::TopicHandle    topic = service.createTopic("mytopic", config, validator, selector, serializer);

        // -- Get a producer for the topic
        mofka::BatchSize   batchSize   = mofka::BatchSize::Adaptive();
        mofka::ThreadCount threadCount = mofka::ThreadCount{1};
        mofka::Ordering    ordering    = mofka::Ordering::Strict;
        mofka::Producer    producer    = topic.producer("myproducer", batchSize, threadCount, ordering);

        srand(time(nullptr));

        // -- Initialize some random data to be sent
        std::vector<char> buffer(8000);
        for(auto& c : buffer) c = 'A' + (rand() % 26);

        spdlog::info("test {}", g_events);
        // -- Produce events
        for(size_t i=0; i < g_events; ++i) {
            auto produce_start = Clock::now();
            auto j = rand() % 100;
            mofka::Metadata metadata = fmt::format("{{\"id\": {}, \"value\": {}}}", i, j);
            mofka::Data data{buffer.data() + i*8, 8};
            spdlog::info("Sending event {} with metadata {} and data {}",
                         i, metadata.string(), std::string_view{buffer.data() + i*8, 8});
            auto future = producer.push(metadata, data);
            // The future can be waited on using future.wait().
            // Here we simply drop it and flush the producer ever 100 events
            if(i % 100 == 0) producer.flush();
            auto produce_end = Clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(produce_end - produce_start).count();
            outfile << "mofka," << i << ",produce," << std::string_view{buffer.data() + i*8, 8}.size() << "," << duration << std::endl;
            spdlog::info("Producing event took {} nanoseconds.", duration);
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
                false, "info", &allowedLogLevels
        );
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
        g_log_level = logLevel.getValue();
        g_events = events.getValue();
        g_benchmark = benchmark.getValue();

    } catch(TCLAP::ArgException &e) {
        std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl;
        exit(-1);
    }
}