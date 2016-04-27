using DbWrapper.Contracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DbWrapper.SqlServer {

	[Serializable]
	public class DynamicSqlQuery : DynamicSqlCommand {

		public DynamicSqlQuery(Database db) 
			: base(db) {

		}

		public override void CreateCommand() {
			throw new NotImplementedException();
		}

		protected override void CreateJoinSection() {
			throw new NotImplementedException();
		}

		protected override void CreateWhereSection() {
			throw new NotImplementedException();
		}

	}
}
