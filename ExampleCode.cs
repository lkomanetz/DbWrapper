using namespace NewCodeStyleSample {

	public class Program {
		
		public static void Main(string[] args) {
			Record rec = new Record("records", db);
			rec.AddQuery("SysId", "=", 1, ClauseType.And);
			rec.AddQuery("Users.UserId", "=", "lkomanetz");
		}

	}


}
