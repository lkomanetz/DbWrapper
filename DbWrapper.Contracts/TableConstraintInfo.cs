using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace DbWrapper.Contracts {

	public class TableConstraintInfo {

		private string _column;
		private bool _isPrimary;

		/// <summary>
		/// Default constructor
		/// </summary>
		public TableConstraintInfo() {
			_column = String.Empty;
			_isPrimary = false;
		}

		public string Column {
			get { return _column; }
			set { _column = value; }
		}

		public bool IsPrimaryKey {
			get { return _isPrimary; }
			set { _isPrimary = value; }
		}

	}

}