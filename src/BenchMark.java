public class BenchMark {

    public static void main(String[] args) {
        BenchMark benchmark = new BenchMark();
        benchmark.runBenchMark(5000000);
    }

    public void runBenchMark(int size) { //size of tuples, whether primary key is sorted
        String tableName = "benchmark";
        DAO dao = new DAO();
        dao.init();
        dao.createTable(tableName);

        dao.insertAllData(size, true);  //true indicate that primary key sorted when insert
        System.out.println("sorted primary key:");
        dao.queryBySecondIdx("columnA", "columnB");

        dao.clearTable(tableName);                        //need to clear table because we need to insert value again
        dao.insertAllData(size, false);
        System.out.println("unsorted primary key:");
        dao.queryBySecondIdx("columnA", "columnB");
    }

}
