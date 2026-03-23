import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

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

public class bai2 extends Configured implements Tool {

    private static final String TAG_MOVIE = "M";
    private static final String TAG_RATING = "R";

    // 1. Mapper xử lý file movies.txt cần có MovieID, Title và Genres
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                outKey.set(parts[0].trim());
                String genres = parts[parts.length - 1].trim();
                outValue.set(TAG_MOVIE + "," + genres);
                context.write(outKey, outValue);
            }
        }
    }

    // 2. Mapper xử lý các file ratings.txt
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

    // 3. Reducer tổng hợp điểm theo Genre
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        // Sử dụng TreeMap để lưu trữ và tự động sắp xếp tên Genre theo thứ tự A-Z
        // Value của Map là một mảng double: [Tổng điểm, Tổng số lượt đánh giá]
        private TreeMap<String, double[]> genreStats = new TreeMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String genres = "";
            double sumRating = 0.0;
            int count = 0;

            // Phân loại dữ liệu nhận được từ Mapper
            for (Text val : values) {
                String valStr = val.toString();

                if (valStr.startsWith(TAG_MOVIE + ",")) {
                    genres = valStr.substring(2);
                } else if (valStr.startsWith(TAG_RATING + ",")) {
                    try {
                        sumRating += Double.parseDouble(valStr.substring(2));
                        count++;
                    } catch (NumberFormatException e) {
                    }
                }
            }

            // Nếu phim có đánh giá và có thông tin thể loại
            if (count > 0 && !genres.isEmpty() && !genres.equals("(no genres listed)")) {
                // Tách các thể loại bằng dấu "|"
                String[] genreArray = genres.split("\\|");

                // Cộng dồn điểm và lượt đánh giá vào biến tổng của từng thể loại
                for (String g : genreArray) {
                    g = g.trim();
                    genreStats.putIfAbsent(g, new double[]{0.0, 0.0});

                    double[] stats = genreStats.get(g);
                    stats[0] += sumRating;
                    stats[1] += count;
                }
            }
        }

        // 4. In kết quả cuối cùng khi đã duyệt qua toàn bộ các phim
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, double[]> entry : genreStats.entrySet()) {
                String genre = entry.getKey();
                double totalSum = entry.getValue()[0];
                int totalCount = (int) entry.getValue()[1];

                double avgRating = totalSum / totalCount;

                outKey.set(genre);
                outValue.set(String.format("Avg: %.2f, Count: %d", avgRating, totalCount));
                context.write(outKey, outValue);
            }
        }
    }

    // 5. Cấu hình Job 
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: bai2 <movies_file> <ratings_file_1> [ratings_file_2 ...] <output_dir>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Analyze Rating By Genre");
        job.setJarByClass(bai2.class);

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
        int exitCode = ToolRunner.run(new Configuration(), new bai2(), args);
        System.exit(exitCode);
    }
}