using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;

namespace DbWrapper {
	public class FlexObject {

		private Hashtable _properties; // Dynamic list of properties for the FlexObject
		private Hashtable _propListInfo;
		private List<TableConstraintInfo> _identityColumns;

		/// <summary>
		/// Default constructor.  Sets the size of each data page automatically
		/// to 25 records per page.
		/// </summary>
		public FlexObject() {
			_properties = new Hashtable();
			_propListInfo = new Hashtable();

			if (_identityColumns == null)
				_identityColumns = new List<TableConstraintInfo>();
		}

		/// <summary>
		/// Dynamic property that is filled with information using the basic concepts
		/// of reflection.  The name of the property is the name of the column and the
		/// object stored is of whatever type is in the database.
		/// </summary>
		/// <param name="propName"></param>
		/// <returns></returns>
		public object this[string propName] {
			get {
				if (_properties.ContainsKey(propName))
					return _properties[propName];
				else
					return null;
			}
			set { _properties[propName] = value; }
		}

		/// <summary>
		/// Gets the list of properties for the single record object.
		/// </summary>
		public Hashtable Properties {
			get { return _properties; }
			internal set { this._properties = value; }
		}

		public Hashtable PropertyListInfo {
			get { return _propListInfo; }
			internal set { this._propListInfo = value; }
		}

		/// <summary>
		/// Gets or sets the column name that is the designated
		/// primary key.  This is used mostly for updates since
		/// you can't update identify values which are usually
		/// identity keys anyway.
		/// </summary>
		public List<TableConstraintInfo> IdentityColumns {
			get { return this._identityColumns; }
			set { this._identityColumns = value; }
		}

		/// <summary>
		/// Returns the object from the property hash table as the type
		/// specified.  Returns the default value of the type specified
		/// if the property doesn't exist or if it is null.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="propName"></param>
		/// <returns></returns>
		public T Get<T>(string propName) {
			return CastValue<T>(propName);
		}

		/// <summary>
		/// Sets the value of a property.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="propName"></param>
		/// <param name="newVal"></param>
		public void Set<T>(string propName, T newVal) {
			if (_properties.ContainsKey(propName))
				_properties[propName] = newVal;
		}

		private T CastValue<T>(string propName) {
			if (_properties.ContainsKey(propName) &&
				_properties[propName] != null) {
				T obj;
				try {
					obj = (T)_properties[propName];
				}
				catch (InvalidCastException) {
					TypeConverter converter = TypeDescriptor.GetConverter(typeof(T));
					obj = converter.CanConvertFrom(typeof(string)) ?
						(T)converter.ConvertFrom(_properties[propName]) :
						default(T);
				}

				return obj;
			}
			else {
				return default(T);
			}
		}
	}
}