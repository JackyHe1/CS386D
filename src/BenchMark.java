public class BenchMark {

    public static void main(String[] args) {
        BenchMark benchmark = new BenchMark();
        benchmark.runBenchMark(5000000);
    }

    public void runBenchMark(int size) { //size of tuples, whether primary key is sorted
        String tableName = "benchmark";
        DAO dao = new DAO();
        dao.init();
        dao.generateData(size);

        dao.loadDataAndQuery(tableName,"columnA", "columnB", true);

        dao.loadDataAndQuery(tableName,"columnA", "columnB", false);
    }

}
