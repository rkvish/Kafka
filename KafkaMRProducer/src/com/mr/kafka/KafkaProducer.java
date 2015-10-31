package com.mr.kafka;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Generate the official GraySort input data set. The user specifies the number
 * of rows and the output directory and this class runs a map/reduce program to
 * generate the data. The format of the data is:
 * <ul>
 * <li>(10 bytes key) (constant 2 bytes) (32 bytes rowid) (constant 4 bytes) (48
 * bytes filler) (constant 4 bytes)
 * <li>The rowid is the right justified row id as a hex number.
 * </ul>
 * 
 * <p>
 * To run the program: hadoop jar
 * TeraGenTweets-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 * com.generate.teragen.tweets.TeraGen 1000000 tweets localhost:9092
 */
public class KafkaProducer extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(KafkaProducer.class);

	public static enum Counters {
		CHECKSUM
	}

	public static final String NUM_ROWS = "mapreduce.terasort.num-rows";

	/**
	 * An input format that assigns ranges of longs to each mapper.
	 */
	static class RangeInputFormat extends
			InputFormat<LongWritable, NullWritable> {

		/**
		 * An input split consisting of a range on numbers.
		 */
		static class RangeInputSplit extends InputSplit implements Writable {
			long firstRow;
			long rowCount;

			public RangeInputSplit() {
			}

			public RangeInputSplit(long offset, long length) {
				firstRow = offset;
				rowCount = length;
			}

			public long getLength() throws IOException {
				return 0;
			}

			public String[] getLocations() throws IOException {
				return new String[] {};
			}

			public void readFields(DataInput in) throws IOException {
				firstRow = WritableUtils.readVLong(in);
				rowCount = WritableUtils.readVLong(in);
			}

			public void write(DataOutput out) throws IOException {
				WritableUtils.writeVLong(out, firstRow);
				WritableUtils.writeVLong(out, rowCount);
			}
		}

		/**
		 * A record reader that will generate a range of numbers.
		 */
		static class RangeRecordReader extends
				RecordReader<LongWritable, NullWritable> {
			long startRow;
			long finishedRows;
			long totalRows;
			LongWritable key = null;

			public RangeRecordReader() {
			}

			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
				startRow = ((RangeInputSplit) split).firstRow;
				finishedRows = 0;
				totalRows = ((RangeInputSplit) split).rowCount;
			}

			public void close() throws IOException {
				// NOTHING
			}

			public LongWritable getCurrentKey() {
				return key;
			}

			public NullWritable getCurrentValue() {
				return NullWritable.get();
			}

			public float getProgress() throws IOException {
				return finishedRows / (float) totalRows;
			}

			public boolean nextKeyValue() {
				if (key == null) {
					key = new LongWritable();
				}
				if (finishedRows < totalRows) {
					key.set(startRow + finishedRows);
					finishedRows += 1;
					return true;
				} else {
					return false;
				}
			}

		}

		public RecordReader<LongWritable, NullWritable> createRecordReader(
				InputSplit split, TaskAttemptContext context)
				throws IOException {
			return new RangeRecordReader();
		}

		/**
		 * Create the desired number of splits, dividing the number of rows
		 * between the mappers.
		 */
		public List<InputSplit> getSplits(JobContext job) {
			long totalRows = getNumberOfRows(job);
			// Set the number of mappers
			int numSplits = 2;
			LOG.info("Generating " + totalRows + " using " + numSplits);
			List<InputSplit> splits = new ArrayList<InputSplit>();
			long currentRow = 0;
			for (int split = 0; split < numSplits; ++split) {
				long goal = (long) Math.ceil(totalRows * (double) (split + 1)
						/ numSplits);
				splits.add(new RangeInputSplit(currentRow, goal - currentRow));
				currentRow = goal;
			}
			return splits;
		}

	}

	static long getNumberOfRows(JobContext job) {
		return job.getConfiguration().getLong(NUM_ROWS, 0);
	}

	static void setNumberOfRows(Job job, long numRows) {
		job.getConfiguration().setLong(NUM_ROWS, numRows);
	}

	/**
	 * The Mapper class that given a row number, will generate the appropriate
	 * output line.
	 */
	public static class SortGenMapper extends
			Mapper<LongWritable, NullWritable, NullWritable, NullWritable> {
		
		// generate some random messages
		Random rand = new Random();
		
		
		String tweetName[] = { "@test1", "@test2", "@test3", "@test4", "@test5" };
		int nameCnt = tweetName.length;
		String tweetMsg[] = {
				"Apache Spark is the Taylor Swift of big data software.",
				"Got a new twitch up and running!",
				"Forked Storm and Spark Streaming and merged them to Stork Streaming",
				"DB writes are executed lazily with Spark Streaming",
				"From On-Premise to Cloud, Monolithic to Microservices, ETL to Spark Streaming." };
		int msgCnt = tweetMsg.length;

		public void map(LongWritable row, NullWritable ignored, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String topic = "tweets"; // conf.get("kafkaTopic");
			String brokers = "localhost:9092"; //conf.get("brokerList");
			
			// Configure the Kafka Properties
			Properties props = new Properties();
			props.put("metadata.broker.list", brokers);
			props.put("serializer.class", "kafka.serializer.StringEncoder");
			props.put("request.required.acks", "0");

			ProducerConfig config = new ProducerConfig(props);
			Producer<String, String> producer = new Producer<String, String>(
					config);
			// Generate fake messages
			StringBuilder strbuffer = new StringBuilder();
			DateFormat dateFormat = new SimpleDateFormat("yyyMMddHHmmss");
			strbuffer.append("name:" + tweetName[rand.nextInt(nameCnt)]
					+ "~tweet:" + tweetMsg[rand.nextInt(msgCnt)]
					+ "~timestamp:" + dateFormat.format(new java.util.Date()));
			KeyedMessage<String, String> msg = new KeyedMessage<String, String>(
					topic, strbuffer.toString());
			producer.send(msg);
			strbuffer.setLength(0);

		}
	}

	private static void usage() throws IOException {
		System.err
				.println("Usage : teragen <num rows to generate> <kafka topic> <kafka brokers>");
	}

	/**
	 * Parse a number that optionally has a postfix that denotes a base.
	 * 
	 * @param str
	 *            an string integer with an option base {k,m,b,t}.
	 * @return the expanded value
	 */
	private static long parseHumanLong(String str) {
		char tail = str.charAt(str.length() - 1);
		long base = 1;
		switch (tail) {
		case 't':
			base *= 1000 * 1000 * 1000 * 1000;
			break;
		case 'b':
			base *= 1000 * 1000 * 1000;
			break;
		case 'm':
			base *= 1000 * 1000;
			break;
		case 'k':
			base *= 1000;
			break;
		default:
		}
		if (base != 1) {
			str = str.substring(0, str.length() - 1);
		}
		return Long.parseLong(str) * base;
	}

	/**
	 * @param args the cli arguments
	 *            
	 */
	public int run(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {

		if (args.length != 3) {
			usage();
			return 2;
		}
		// Configure the Kafka Properties
		Configuration conf = new Configuration();
		long milliSeconds = 1000 * 60 * 60;
		conf.setLong("mapred.task.timeout", milliSeconds);
		conf.set("kafkaTopic", args[1]);
		conf.set("brokerList", args[2]);

		// Configure the MR Job settings
		Job job = Job.getInstance(getConf());
		setNumberOfRows(job, parseHumanLong(args[0]));
		job.setJobName("teragen");
		job.setJarByClass(KafkaProducer.class);
		job.setMapperClass(SortGenMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(RangeInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner
				.run(new Configuration(), new KafkaProducer(), args);
		System.exit(res);
	}

}
