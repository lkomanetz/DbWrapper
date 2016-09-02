using namespace NewCodeStyleSample {

	public class Program {
		
		public static void Main(string[] args) {
			DynamicDatabase db = new DynamicDatabase(
				"connectionString",
				SqlEngine.SqlServer,
				"username",
				"password"
			);

			db.WithTable("records")
				.Where("<column>", "<operator>", value)
				.And("<column>", "<operator>", value)
				.Or("<column>", "<operator>", value)
				.JoinOn("users", JoinType.Inner, 

		}

	}

	public class DynamicDatabase {

		public abstract DynamicTable WithTable(string tableName);

	}

}
