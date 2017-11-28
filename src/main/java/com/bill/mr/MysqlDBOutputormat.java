package com.bill.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bill.util.OpUtils;

/**
 * @ProjectName DBOutputormatDemo
 * @ClassName MysqlDBOutputormatDemo
 * @Description TODO
 */
@SuppressWarnings({ "unused", "deprecation" })
public class MysqlDBOutputormat extends Configured implements Tool {
	/**
	 * 实现DBWritable
	 * TblsWritable需要向mysql中写入数据
	 */
	public static class TblsWritable implements Writable, DBWritable {
		
		String b_key;
		String b_value;

		public TblsWritable() {
		}

		public TblsWritable(String b_key, String b_value) {
			this.b_key = b_key;
			this.b_value = b_value;
		}

		@Override
		public void write(PreparedStatement statement) throws SQLException {
			statement.setString(1, this.b_key);
			statement.setString(2, this.b_value);
		}

		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
			this.b_key = resultSet.getString(1);
			this.b_value = resultSet.getString(2);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(this.b_key);
			out.writeUTF(this.b_value);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.b_key = in.readUTF();
			this.b_value = in.readUTF();
		}

		public String toString() {
			return new String(this.b_key + " " + this.b_value);
		}
	}

	public static class StudentMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class StudentReducer extends Reducer<LongWritable, Text, TblsWritable, TblsWritable> {
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// values只有一个值，因为key没有相同的
			StringBuilder value = new StringBuilder();
			for (Text text : values) {
				value.append(text);
			}
			
			String split_reg = OpUtils.getArgs("split_reg");
			
			String[] studentArr = value.toString().split(split_reg);
			
			if (StringUtils.isNotBlank(studentArr[0])) {

				String name = studentArr[0].trim();
				String	age = studentArr[1].trim();
				
				//入redis
				//OpUtils.jedisCluster.set(name, age);
				
				String redis_set = OpUtils.getArgs("redis_set");
				
				OpUtils.jedisCluster.hset(redis_set, name, age);
				//mr导入到mysql中
				context.write(new TblsWritable(name, age), null);
			}
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// 读取配置文件
		Configuration conf = new Configuration();
		
		DBConfiguration.configureDB(conf, OpUtils.getArgs("jdbcDriver"), OpUtils.getArgs("jdbcUrl"), OpUtils.getArgs("jdbcUsername"), OpUtils.getArgs("jdbcPasswd"));

		// 新建一个任务
		Job job = Job.getInstance(conf, OpUtils.getArgs("jobName"));
		// 设置主类
		job.setJarByClass(MysqlDBOutputormat.class);

		// 输入路径
		FileInputFormat.addInputPath(job, new Path(arg0[0]));

		// Mapper
		job.setMapperClass(StudentMapper.class);
		// Reducer
		job.setReducerClass(StudentReducer.class);

		// mapper输出格式
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		// 输出格式
		job.setOutputFormatClass(DBOutputFormat.class);

		String tb = OpUtils.getArgs("tblName");
		String field = OpUtils.getArgs("tblfields");
		
		String[] split = field.split(",");
		
		// 输出到哪些表、字段
		DBOutputFormat.setOutput(job, tb, split);

		// 提交任务
		return job.waitForCompletion(true) ? 0 : 1;
	}

}