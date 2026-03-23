import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class bai1 extends Configured implements Tool {

    private static final String TAG_MOVIE = "M";
    private static final String TAG_RATING = "R";

    // 1. Mapper xử lý file movies.txt chỉ cần có MovieID và Title
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

    // 2. Mapper xử lý các file ratings
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                outKey.set(parts[1].trim());
                outValue.set(TAG_RATING + "," + parts[2].trim());
                context.write(outKey, outValue);
            }
        }
    }

    // 3. Reducer tổng hợp, tính điểm và tìm Max
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        // Biến lớp theo yêu cầu
        private String maxMovie = "";
        private double maxRating = -1.0;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = "Unknown Movie";
            double sumRating = 0.0;
            int count = 0;

            for (Text val : values) {
                String valStr = val.toString();

                if (valStr.startsWith(TAG_MOVIE)) {
                    movieTitle = valStr.substring(2);
                } else if (valStr.startsWith(TAG_RATING)) {
                    try {
                        sumRating += Double.parseDouble(valStr.substring(2));
                        count++;
                    } catch (NumberFormatException e) {
                    }
                }
            }

            // Xuất ra kết quả cho các phim miễn là có người đánh giá (count > 0)
            if (count > 0) {
                double avgRating = sumRating / count;

                outKey.set(movieTitle);
                outValue.set(String.format("AverageRating: %.2f (TotalRatings: %d)", avgRating, count));
                context.write(outKey, outValue);

                // Tìm phim có điểm trung bình cao nhất nếu lượt đánh giá >= 5
                if (count >= 5) {
                    if (avgRating > maxRating) {
                        maxRating = avgRating;
                        maxMovie = movieTitle;
                    }
                }
            }
        }

        // Xuất ra kết luận cuối cùng trong hàm cleanup()
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                String finalResult = String.format("%s is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings.", maxMovie, maxRating);
                outKey.set(finalResult);
                outValue.set("");
                context.write(outKey, outValue);
            }
        }
    }

    // 4. Cấu hình Job 
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: bai1 <movies_file> <ratings_file_1> [ratings_file_2 ...] <output_dir>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Calculate Movie Average Rating");
        job.setJarByClass(bai1.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);

        for (int i = 1; i < args.length - 1; i++) {
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
        int exitCode = ToolRunner.run(new Configuration(), new bai1(), args);
        System.exit(exitCode);
    }
}