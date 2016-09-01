using DbWrapper.Read;
using DbWrapper.Contracts;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

namespace DbWrapper.Serializer {
	public static class RecordSerializer {
		/*
		 * Private class variables
		 */
		private static XmlDocument _xmlDoc;
		private static Record _record;

		/*
		 * Constructors
		 */
		static RecordSerializer() {
			Init();
		}

		private static void Init() {
			_xmlDoc = new XmlDocument();
			_record = new Record();
		}

		/// <summary>
		/// Serializes the record object into an XML document
		/// </summary>
		public static XmlDocument Serialize(ref Record rec) {
			_record = rec;
			XmlDeclaration dec = _xmlDoc.CreateXmlDeclaration("1.0", "UTF-8", null);
			_xmlDoc.AppendChild(dec);

			XmlElement root = _xmlDoc.CreateElement("record");
			root.AppendChild(SerializeProperties());
			root.AppendChild(SerializeQuery());

			_xmlDoc.AppendChild(root);
			return _xmlDoc;
		}

		/// <summary>
		/// Recreates the Record object from the serialized XML
		/// document.
		/// </summary>
		/// <param name="rec"></param>
		/// <param name="xmlPath"></param>
		/// <returns></returns>
		public static Record Deserialize(XmlDocument xmlDoc, out Record rec) {
			XmlNode table = xmlDoc.SelectSingleNode("//query/table");
			XmlNode database = xmlDoc.SelectSingleNode("//query/database");

			string databaseType = database.Attributes[2].InnerText;
			DatabaseEngine engine = (DatabaseEngine)Enum.Parse(typeof(DatabaseEngine),
																databaseType,
																true);

			DynamicDatabase db = new DynamicDatabase(database.Attributes[0].InnerText,
									   database.Attributes[1].InnerText,
									   engine,
									   database.Attributes[3].InnerText);

			rec = new Record(table.InnerText, db);
			DeserializeProperties(ref xmlDoc, ref rec);
			DeserializeClauses(ref xmlDoc, ref rec);
			DeserializeJoins(ref xmlDoc, ref rec);

			return rec;
		}

		/// <summary>
		/// Loops through all of the record's properties and serializes
		/// them into XML elements.
		/// </summary>
		/// <returns></returns>
		private static XmlElement SerializeProperties() {
			XmlElement properties = _xmlDoc.CreateElement("properties");

			foreach (string key in _record.Properties.Keys) {
				XmlElement property = _xmlDoc.CreateElement("property");
				property.SetAttribute("Name", key);
				property.SetAttribute("Value", _record.Properties[key].ToString());
				property.SetAttribute("Type", _record.Properties[key].GetType().ToString());

				properties.AppendChild(property);
			}

			return properties;
		}

		/// <summary>
		/// Loops through all of the <property/> elements and sets each property
		/// in the new record.
		/// </summary>
		/// <param name="xmlDoc"></param>
		/// <param name="rec"></param>
		private static void DeserializeProperties(ref XmlDocument xmlDoc,
												  ref Record rec) {
			XmlNodeList properties = xmlDoc.SelectNodes("//property");

			for (short i = 0; i < properties.Count; i++) {
				XmlNode property = properties[i];
				string attributeName = property.Attributes["Name"].InnerText;
				rec[attributeName] = property.Attributes["Value"].InnerText;
			}
		}

		/// <summary>
		/// Loops through all of the <set /> elements in the criteria root and
		/// recreates all of the queries for the record object.
		/// </summary>
		/// <param name="xmlDoc"></param>
		/// <param name="rec"></param>
		private static void DeserializeClauses(ref XmlDocument xmlDoc,
											   ref Record rec) {
			XmlNodeList whereClauses = xmlDoc.SelectNodes("//criteria/set");

			for (short i = 0; i < whereClauses.Count; i++) {
				XmlNode clause = whereClauses[i];
				string clauseStr = String.Format("{0}/{1}/{2}",
												 clause.Attributes["column"].InnerText,
												 clause.Attributes["operator"].InnerText,
												 clause.Attributes["value"].InnerText);

				switch (clause.Attributes["type"].InnerText) {
					case "And":
						rec.AddQuery(clauseStr, ClauseType.And);
						break;
					case "Or":
						rec.AddQuery(clauseStr, ClauseType.Or);
						break;
					default:
						rec.AddQuery(clauseStr);
						break;
				}

			}
		}

		/// <summary>
		/// Loops through all of the <join /> elements and recreates all of the joins
		/// for the record object.
		/// </summary>
		/// <param name="xmlDoc"></param>
		/// <param name="rec"></param>
		private static void DeserializeJoins(ref XmlDocument xmlDoc,
											 ref Record rec) {
			XmlNodeList joins = xmlDoc.SelectNodes("//joins/join");

			for (short i = 0; i < joins.Count; i++) {
				XmlNode join = joins[i];
				Join joinObj = new Join();

				foreach (XmlNode child in join.ChildNodes) {
					switch (child.Name) {
						case "type":
							JoinType type = JoinType.Inner;
							switch (child.InnerText) {
								case "Inner":
									type = JoinType.Inner;
									break;
								case "Outer":
									type = JoinType.Outer;
									break;
								case "Left":
									type = JoinType.Left;
									break;
								case "Full":
									type = JoinType.Full;
									break;
								case "Right":
									type = JoinType.Right;
									break;
							}
							joinObj.Type = type;
							break;
						case "table":
							joinObj.Table = join.InnerText;
							break;
						case "on":
							joinObj.JoinColumn = join.InnerText;
							break;
						case "with":
							joinObj.Column = join.InnerText;
							break;
					}
				}
				rec.AddJoin(joinObj.Table,
							joinObj.JoinColumn,
							joinObj.Column,
							joinObj.Type);
			}
		}

		/// <summary>
		/// Generates the Query section of the XML document
		/// </summary>
		/// <returns></returns>
		private static XmlElement SerializeQuery() {
			XmlElement query = _xmlDoc.CreateElement("query");

			XmlElement database = _xmlDoc.CreateElement("database");
			XmlElement table = _xmlDoc.CreateElement("table");

			table.InnerText = _record.Table;
			database.SetAttribute("Server", _record.Database.Server);
			database.SetAttribute("Catalog", _record.Database.Catalog);
			database.SetAttribute("Engine", _record.Database.Engine.ToString());
			database.SetAttribute("Username", _record.Database.Username);
			database.SetAttribute("Password", _record.Database.Password);

			// Create the XML elements for each column
			XmlElement columns = _xmlDoc.CreateElement("columns");
			foreach (string key in _record.Properties.Keys) {
				XmlElement column = _xmlDoc.CreateElement("column");
				column.SetAttribute("Name", key);
				columns.AppendChild(column);
			}

			// Create the XML elements for each WHERE clause
			XmlElement criteria = _xmlDoc.CreateElement("criteria");
			foreach (WhereClause clause in _record.Query.Clauses) {
				string column = String.Format("{0}", clause.Column);
				XmlElement set = _xmlDoc.CreateElement("set");
				set.SetAttribute("column", column);
				set.SetAttribute("operator", clause.Operator);
				set.SetAttribute("value", clause.Value.ToString());
				set.SetAttribute("type", clause.Type.ToString());

				criteria.AppendChild(set);
			}

			// Create the XML elements for each JOIN
			XmlElement joins = _xmlDoc.CreateElement("joins");
			foreach (string key in _record.Query.Joins.Keys) {
				Join joinObj = _record.Query.Joins[key];
				XmlElement join = _xmlDoc.CreateElement("join");

				XmlElement type = _xmlDoc.CreateElement("type");
				type.InnerText = joinObj.Type.ToString();

				XmlElement joinTable = _xmlDoc.CreateElement("table");
				joinTable.InnerText = joinObj.Table;

				XmlElement joinOn = _xmlDoc.CreateElement("on");
				joinOn.InnerText = joinObj.Column;

				XmlElement joinWith = _xmlDoc.CreateElement("with");
				joinWith.InnerText = joinObj.JoinColumn;

				join.AppendChild(type);
				join.AppendChild(joinTable);
				join.AppendChild(joinOn);
				join.AppendChild(joinWith);

				joins.AppendChild(join);
			}

			// Append each element to the query XML element
			query.AppendChild(table);
			query.AppendChild(database);
			query.AppendChild(columns);
			query.AppendChild(criteria);
			query.AppendChild(joins);

			return query;
		}
	}
}