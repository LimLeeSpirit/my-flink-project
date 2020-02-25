package myflink;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author LimLee
 * @date 10:49 2020/2/17
 * @description
 * 有一个数据源，监控系统中订单的情况，当有新订单时，
 * 它使用 Tuple2<String, Integer> 输出订单中商品的类型和交易额。
 * 然后，我们希望实时统计每个类别的交易额，以及实时统计全部类别的交易额
 *
 * 当我们调用 DataStream.map 算法时，Flink 在底层会创建一个 Transformation 对象，
 * 这一对象就代表我们计算逻辑图中的节点。它其中就记录了我们传入的 MapFunction，
 * 也就是 UDF（User Define Function）。随着我们调用更多的方法，我们创建了更多的 DataStream 对象，
 * 每个对象在内部都有一个 Transformation 对象，这些对象根据计算依赖关系组成一个图结构，
 * 就是我们的计算图。后续 Flink 将对这个图结构进行进一步的转换，从而最终生成提交作业所需要的 JobGraph。
 */
public class FlinkGroupProcessingTimeWindowDemo {
    // 自定义数据源
    private static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {
        private volatile boolean isRunning = true;

        @Override
        // Flink 在运行时对 Source 会直接调用该方法，该方法需要不断的输出数据，从而形成初始的流。
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 * 5);
                String key = "类别" + (char) ('A' + random.nextInt(3));
                int value = random.nextInt(10) + 1;

                System.out.println(String.format("Emits\t(%s, %d)", key, value));
                // 通过 ctx.collect 方法进行发送
                ctx.collect(new Tuple2<>(key, value));
            }
        }

        @Override
        // 当 Flink 需要 Cancel Source Task 的时候会调用该方法，
        // 我们使用一个 Volatile 类型的变量来标记和控制执行的状态。
        public void cancel() {
            isRunning = false;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(2);

        // 添加数据源
        DataStream<Tuple2<String, Integer>> ds = env.addSource(new DataSource());

        // 使用 KeyBy 按 Tuple 的第 1 个字段（即商品类型）对输入流进行分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = ds.keyBy(0);

        // 并对每一个 Key 对应的记录的第 2 个字段（即成交量）进行求合
        // 并通过 KeyBy 对所有记录返回同一个 Key，将所有记录分到同一个组中，从而可以全部发送到同一个实例上。
        keyedStream
            .sum(1)
            .keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
                @Override
                public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                    return "";
                }
            })
            .fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
                @Override
                // 使用 Fold 方法来在算子中维护每种类型商品的成交量
                // 接收一个初始值，然后当后续流中每条记录到达的时候，
                // 算子会调用所传递的 FoldFunction 对初始值进行更新，并发送更新后的值
                public HashMap<String, Integer> fold(HashMap<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
                    accumulator.put(value.f0, value.f1);
                    return accumulator;
                }
            })
            .addSink(new SinkFunction<HashMap<String, Integer>>() {
                @Override
                public void invoke(HashMap<String, Integer> value, Context context) throws Exception {
                    // 每个类型的商品成交量
                    System.out.println(value);
                    // 商品成交总量
                    System.out.println(value.values().stream().mapToInt(v -> v).sum());
                }
            });
        env.execute();
    }
}
