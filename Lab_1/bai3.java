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

public class bai3 extends Configured implements Tool {

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
                outKey.set(parts[0].trim());
                outValue.set(TAG_MOVIE + "," + parts[1].trim());
                context.write(outKey, outValue);
            }
        }
    }

    // 2. Mapper xử lý ratings.txt và đọc trước users.txt vào bộ nhớ (Map-side Join)
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        private HashMap<String, String> userGenderMap = new HashMap<>();

        // Hàm setup chạy 1 lần duy nhất trước khi map() bắt đầu quét file
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
                    if (parts.length >= 2) {
                        // Lưu UserID -> Giới tính vào HashMap
                        userGenderMap.put(parts[0].trim(), parts[1].trim());
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

                // Lấy giới tính từ bộ nhớ tạm, nếu không có mặc định là Unknown
                String gender = userGenderMap.getOrDefault(userId, "Unknown");

                outKey.set(movieId);
                // Gửi cho Reducer gói dữ liệu: R, GiớiTính, Điểm
                outValue.set(TAG_RATING + "," + gender + "," + rating);
                context.write(outKey, outValue);
            }
        }
    }

    // 3. Reducer tổng hợp và tính điểm trung bình
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = "Unknown Movie";
            double sumMale = 0.0, sumFemale = 0.0;
            int countMale = 0, countFemale = 0;

            for (Text val : values) {
                String valStr = val.toString();

                if (valStr.startsWith(TAG_MOVIE + ",")) {
                    movieTitle = valStr.substring(2);
                } else if (valStr.startsWith(TAG_RATING + ",")) {
                    String[] parts = valStr.substring(2).split(",");
                    if (parts.length == 2) {
                        String gender = parts[0];
                        try {
                            double rating = Double.parseDouble(parts[1]);
                            if (gender.equalsIgnoreCase("M") || gender.equalsIgnoreCase("Male") || gender.equals("1")) {
                                sumMale += rating;
                                countMale++;
                            } else if (gender.equalsIgnoreCase("F") || gender.equalsIgnoreCase("Female") || gender.equals("0")) {
                                sumFemale += rating;
                                countFemale++;
                            }
                        } catch (NumberFormatException e) {
                        }
                    }
                }
            }

            // Chỉ xuất kết quả nếu phim đó có ít nhất 1 người đánh giá
            if (countMale > 0 || countFemale > 0) {
                String maleStr = (countMale > 0) ? String.format("%.2f", sumMale / countMale) : "N/A ";
                String femaleStr = (countFemale > 0) ? String.format("%.2f", sumFemale / countFemale) : "N/A ";

                outKey.set(String.format("%-35s", movieTitle));
                outValue.set(String.format("Male: %s, Female: %s", maleStr, femaleStr));

                context.write(outKey, outValue);
            }
        }
    }

    // 4. Job config
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: bai3 <users_file> <movies_file> <ratings_file_1> [<ratings_file_2> ...] <output_dir>");
            return -1;
        }

        Configuration conf = getConf();
        // Chuyển đường dẫn file users vào cho Mapper 2 đọc
        conf.set("users.file.path", args[0]);

        Job job = Job.getInstance(conf, "Analyze Rating By Gender");
        job.setJarByClass(bai3.class);

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
        int exitCode = ToolRunner.run(new Configuration(), new bai3(), args);
        System.exit(exitCode);
    }
}