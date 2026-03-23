import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class bai4 extends Configured implements Tool {

    private static final String TAG_MOVIE = "M";
    private static final String TAG_RATING = "R";

    // 1. Mapper xử lý file movies.txt
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",", 3);
            if (parts.length >= 2) {
                outKey.set(parts[0].trim()); // MovieID
                outValue.set(TAG_MOVIE + "," + parts[1].trim()); // Gắn cờ M, Title
                context.write(outKey, outValue);
            }
        }
    }

    // 2. Mapper xử lý ratings.txt và Join Tuổi từ users.txt
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        private HashMap<String, String> userAgeGroupMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String usersFilePath = conf.get("users.file.path");

            if (usersFilePath != null) {
                Path path = new Path(usersFilePath);
                FileSystem fs = path.getFileSystem(conf);
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;

                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 3) {
                        try {
                            String userId = parts[0].trim();
                            int age = Integer.parseInt(parts[2].trim());
                            String ageGroup;

                            // Phân loại nhóm tuổi
                            if (age <= 18) ageGroup = "0-18";
                            else if (age <= 35) ageGroup = "18-35";
                            else if (age <= 50) ageGroup = "35-50";
                            else ageGroup = "50+";

                            userAgeGroupMap.put(userId, ageGroup);
                        } catch (NumberFormatException e) {
                        }
                    }
                }
                br.close();
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                String userId = parts[0].trim();
                String movieId = parts[1].trim();
                String rating = parts[2].trim();


                String ageGroup = userAgeGroupMap.getOrDefault(userId, "Unknown");

                // Chỉ xử lý các nhóm tuổi hợp lệ
                if (!ageGroup.equals("Unknown")) {
                    outKey.set(movieId);
                    outValue.set(TAG_RATING + "," + ageGroup + "," + rating);
                    context.write(outKey, outValue);
                }
            }
        }
    }

    // 3. Reducer tổng hợp và tính điểm trung bình cho 4 nhóm tuổi
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = "Unknown Movie";

            double sum_0_18 = 0.0, sum_18_35 = 0.0, sum_35_50 = 0.0, sum_50_plus = 0.0;
            int count_0_18 = 0, count_18_35 = 0, count_35_50 = 0, count_50_plus = 0;

            for (Text val : values) {
                String valStr = val.toString();

                if (valStr.startsWith(TAG_MOVIE + ",")) {
                    movieTitle = valStr.substring(2);
                } else if (valStr.startsWith(TAG_RATING + ",")) {
                    String[] parts = valStr.substring(2).split(",");
                    if (parts.length == 2) {
                        String ageGroup = parts[0];
                        try {
                            double rating = Double.parseDouble(parts[1]);
                            switch (ageGroup) {
                                case "0-18": sum_0_18 += rating; count_0_18++; break;
                                case "18-35": sum_18_35 += rating; count_18_35++; break;
                                case "35-50": sum_35_50 += rating; count_35_50++; break;
                                case "50+": sum_50_plus += rating; count_50_plus++; break;
                            }
                        } catch (NumberFormatException e) {
                        }
                    }
                }
            }

            // Chỉ xuất nếu phim có ít nhất 1 đánh giá
            if (count_0_18 > 0 || count_18_35 > 0 || count_35_50 > 0 || count_50_plus > 0) {


                String r1 = (count_0_18 > 0) ? String.format("%.2f", sum_0_18 / count_0_18) : "NA";
                String r2 = (count_18_35 > 0) ? String.format("%.2f", sum_18_35 / count_18_35) : "NA";
                String r3 = (count_35_50 > 0) ? String.format("%.2f", sum_35_50 / count_35_50) : "NA";
                String r4 = (count_50_plus > 0) ? String.format("%.2f", sum_50_plus / count_50_plus) : "NA";

                outKey.set(String.format("%-30s", movieTitle));
                outValue.set(String.format("0-18: %-4s 18-35: %-4s 35-50: %-4s 50+: %-4s", r1, r2, r3, r4));

                context.write(outKey, outValue);
            }
        }
    }

    // 4. Job config
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: bai4 <users_file> <movies_file> <ratings_file_1> [<ratings_file_2> ...] <output_dir>");
            return -1;
        }

        Configuration conf = getConf();
        conf.set("users.file.path", args[0]);

        Job job = Job.getInstance(conf, "Analyze Rating By Age");
        job.setJarByClass(bai4.class);

        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);

        for (int i = 2; i < args.length - 1; i++) {
            MultipleInputs.addInputPath(job, new Path(args[i]), TextInputFormat.class, RatingMapper.class);
        }

        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new bai4(), args);
        System.exit(exitCode);
    }
}