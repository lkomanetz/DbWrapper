using System;
using System.ComponentModel;
using System.Data;
using System.Data.Odbc;
using System.Text.RegularExpressions;

namespace DbWrapper.Contracts {
	public enum ClauseType : byte {
		Neither,
		And,
		Or
	}

	public class WhereClause {

		private string _clause;
		private ClauseType _type;
		private Type _dataType;
		private string _column;
		private string _table;
		private string _clauseOperator;
		private object _value;

		public WhereClause(
			string column,
			string clauseOperator,
			object value,
			string table,
			ClauseType type = ClauseType.Neither) {

			_column = column;
			_clauseOperator = clauseOperator;
			_value = value;
			_type = type;
			_table = table;
			this.DataType = value.GetType();
		}

		/// <summary>
		/// Gets or sets the clause
		/// </summary>
		public string Clause {
			get { return this._clause; }
			set { this._clause = value; }
		}

		public string Table {
			get { return this._table; }
			set { this._table = value; }
		}

		/// <summary>
		/// Gets or sets the type of clause
		/// </summary>
		public ClauseType Type {
			get { return this._type; }
			set { this._type = value; }
		}

		public Type DataType {
			get { return this._dataType; }
			private set { this._dataType = value; }
		}

		/// <summary>
		/// Gets or sets the table column used in the clause
		/// </summary>
		public string Column {
			get { return this._column; }
			set { this._column = value; }
		}

		/// <summary>
		/// Gets or sets the operator used in the WHERE statement
		/// </summary>
		public string Operator {
			get { return this._clauseOperator; }
			set { this._clauseOperator = value; }
		}

		/// <summary>
		/// Gets or sets the value for the WHERE statement
		/// </summary>
		public object Value {
			get { return this._value; }
			set { this._value = value; }
		}

		/// <summary>
		/// Breaks apart the WHERE clause into three different parts.
		/// Index 0 = The name of the column
		/// Index 1 = The operator (equal to, less than, etc.)
		/// Index 3 = The value being tested with.
		/// IE:  Name/=/Person means that column "Name" is equal to
		/// the value "Person"
		/// </summary>
		/// <param name="clause"></param>
		/// <returns></returns>
		private void BreakClauseApart() {
			string pattern = @"(^\w+.*)\/([A-Za-z\s*A-Za-z*]+|[^\w]+)\/(.*[^\w]*\w*)";
			Match match = Regex.Match(this._clause, pattern);
			string[] output = new string[3];

			this._column = match.Groups[1].Value;
			this._clauseOperator = match.Groups[2].Value;
			this._value = match.Groups[3].Value;
		}

		public OdbcParameter BuildParameter() {
			OdbcParameter parameter = new OdbcParameter() {
				ParameterName = "?",
				Value = _value
			};

			if (_dataType != null) {
				TypeConverter tc = TypeDescriptor.GetConverter(parameter.DbType);
				if (tc.CanConvertFrom(_dataType)) {
					parameter.DbType = (DbType)tc.ConvertFrom(_dataType.Name);
				}
				else {
					try {
						parameter.DbType = (DbType)tc.ConvertFrom(_dataType.Name);
					}
					catch {
						if (_dataType == typeof(byte[])) {
							parameter.DbType = DbType.Binary;
							parameter.Size = -1;
						}
					}
				}
			}

			return parameter;
		}

	}

}