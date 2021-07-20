var express = require("express");
var router = express.Router();
const fs = require("fs");
const parser = require("pg-query-parser");

/* GET home page. */
router.get("/", function (req, res, next) {
  res.json({});
});

router.get("/getDiagram", function (req, res, next) {
  var sql = req.query.sql;
  var table_table_name = req.query.table_name;

  res.json(sqltoRel(sql, "FINAL_" + table_table_name));
});

router.get("/test", function (req, res, next) {
  var sql = "select * from hube.cdm.person";

  var parsed = parser.parse(sql);
  res.json(parsed);
});

// FUNCTIONS
var intermediateTables = [];

function normalize(inputtext) {
  try {
    var commentsregex = /--.*\n/g;
    return inputtext.replace(commentsregex, "\n");
  } catch (error) {
    return error.toString();
  }
}

function normalizeSelect(inputtext) {
  try {
    inputtext = inputtext.replace(/(\r\n|\n|\r)/gm, "");
    return inputtext.replace(/"/g, "");
  } catch (error) {
    return error.toString();
  }
}

function sqltoRel(sql, table_name = "") {
  var tablesnamesregex = /\w+(?=(\s+(as|AS)\s+\(((.|\n)*?)\)\s*\n))/g;
  var selects3regex =
    /(select|SELECT)(.|\n)+?(?=(\)\s*(\n(,|s)|\s*,(\s|\n)*(\w*\s+(as|AS)\s+\(|select)))|$)/g;
  sql = normalize(sql);

  let tableNames = sql.match(tablesnamesregex);
  tableNames = tableNames != null ? tableNames : [];
  console.log(tableNames);

  tableNames.push(table_name);
  let selects = sql.match(selects3regex);
  let tables = [];
  let allrelations = [];
  for (const i in tableNames) {
    let relations = extractInfo(selects[i], tableNames[i]);
    //let text = jsonToMermaid(output);
    allrelations.push(relations);
    let rel = {
      table_name: tableNames[i],
      compiled_sql: normalizeSelect(selects[i]),
      relations: relToGoJS(relations),
    };

    tables.push(rel);
  }
  let rel = {
    table_name: "ALL",
    compiled_sql: "",
    relations: relToGoJS(allrelations.flat()),
  };
  tables.push(rel);

  return tables;
}

//FUNCTIONS 2

function getNameAndAlias(object) {
  return {
    alias:
      object["alias"] != null
        ? object["alias"]["Alias"]["aliasname"]
        : object["relname"],
    full_name:
      (object["schemaname"] != null ? object["schemaname"] + "." : "") +
      object["relname"],
  };
}
function getTables(obj) {
  if (obj["JoinExpr"] != null) {
    let left = getTables(obj["JoinExpr"]["larg"]);
    let right = getTables(obj["JoinExpr"]["rarg"]);
    return [left, right].flat();
  } else if (obj["RangeVar"] != null) {
    return getNameAndAlias(obj["RangeVar"]);
  }
  return null;
}

function getAllSQLTables(obj) {
  var tables = [];
  obj.forEach((el) => {
    tables.push(getTables(el));
  });
  return tables.flat();
}

function aliasToFull(alias) {
  let out = null;
  intermediateTables.forEach((tab) => {
    if (alias == tab.alias || alias == "") {
      out = tab.full_name;
    }
  });
  return out;
}
function getConstant(obj, final_target, final_table) {
  let val =
    obj.val.String != null
      ? obj.val.String.str
      : obj.val.Integer != null
      ? obj.val.Integer.ival.toString()
      : "null";
  return {
    origin_table: "constants",
    origin_val: val,
    target_val: final_target,
    target_table: final_table,
    comment: "",
  };
}
function getTypeName(obj) {
  return obj.TypeName.names[obj.TypeName.names.length - 1].String.str;
}
function getCase(obj, final_target, final_table) {
  let rel = getValueOrigin(obj.defresult, final_target, final_table);
  rel.comment += "Case..when application.";
  return rel;
}
function getExpr(obj, final_target, final_table) {
  let leftTree = getValueOrigin(obj.lexpr, final_target, final_table);
  let rightTree = getValueOrigin(obj.rexpr, final_target, final_table);

  return [leftTree, rightTree].flat();
}
function getColumRef(obj, final_target, final_table) {
  if (obj.length == 2) {
    return {
      origin_table: aliasToFull(obj[0].String.str),
      origin_val: obj[1].String.str,
      target_val: final_target != false ? final_target : obj[1].String.str,
      target_table: final_table,
      comment: "",
    };
  }
  let original_val =
    obj[0].String != null
      ? obj[0].String.str
      : obj[0].A_Star != null
      ? "*"
      : "Unknown";
  return {
    origin_table: aliasToFull(""),
    origin_val: original_val,
    target_val: final_target != false ? final_target : original_val,
    target_table: final_table,
    comment: "",
  };
}
function getFunc(obj, final_target, final_table) {
  let funcname = obj.funcname["0"].String.str;
  let comment = funcname + " of ";
  let target = funcname;
  let temp = null;
  let bunch = [];
  try {
    obj.args.forEach((element) => {
      temp = getValueOrigin(element, final_target, final_table);
      bunch.push(temp);
      temp.forEach((el) => {
        comment += el.origin_val + " ";
      });
    });
  } catch (error) {
    console.log(error);
  }
  for (var index in bunch) {
    bunch[index][0].comment = comment;
  }
  return bunch.flat();
}

function getValueOrigin(obj, final_target, final_table) {
  if (obj["TypeCast"] != null) {
    let rel = getValueOrigin(obj["TypeCast"].arg, final_target, final_table);
    for (let i in rel) {
      rel[i].comment =
        rel[i].comment != ""
          ? rel[i].comment
          : getTypeName(obj["TypeCast"].typeName);
    }
    return [rel].flat();
  } else if (obj["FuncCall"] != null) {
    return [getFunc(obj["FuncCall"], final_target, final_table)].flat();
  } else if (obj["A_Const"] != null) {
    return [getConstant(obj["A_Const"], final_target, final_table)].flat();
  } else if (obj["ColumnRef"] != null) {
    return [
      getColumRef(obj["ColumnRef"].fields, final_target, final_table),
    ].flat();
  } else if (obj["CaseExpr"] != null) {
    return getCase(obj["CaseExpr"], final_target, final_table);
  } else if (obj["NullTest"] != null) {
    return [
      getValueOrigin(obj["NullTest"].arg, final_target, final_table),
    ].flat();
  } else if (obj["A_Expr"] != null) {
    return [getExpr(obj["A_Expr"], final_target, final_table)].flat();
  }
}

function getRelation(obj, table_name) {
  var targetname = obj.name != null ? obj.name : false;
  return getValueOrigin(obj.val, targetname, table_name);
}

function extractInfo(sql, table_name) {
  var query = parser.parse(sql).query;
  if (query.length == 0) {
    return [];
  }
  intermediateTables = getAllSQLTables(
    query[0].SelectStmt.fromClause != null ? query[0].SelectStmt.fromClause : []
  );
  var targets = query[0].SelectStmt.targetList;
  var relations = [];
  try {
    targets.forEach((element) => {
      relations.push(getRelation(element.ResTarget, table_name));
    });
  } catch (error) {
    console.log(error);
  }
  relations = relations.flat();
  return relations;
}

function relToGoJS(relations) {
  let y = 0;
  let node = {
    key: "",
    fields: [],
  };
  let nodes = {};
  let nodeDataArray = [];
  let linkDataArray = [];

  relations.forEach((rel) => {
    if (rel.origin_table == "constants") {
      nodes = relToField(
        nodes,
        rel.target_table,
        rel.target_val,
        "#" + rel.origin_val
      );
    } else {
      nodes = relToField(nodes, rel.origin_table, rel.origin_val, rel.comment);
      nodes = relToField(nodes, rel.target_table, rel.target_val, "");
      linkDataArray.push({
        from: rel.origin_table,
        fromPort: rel.origin_val,
        to: rel.target_table,
        toPort: rel.target_val,
      });
    }
  });
  pos = { x: {}, y: {} };
  x = 25;
  Object.entries(nodes).forEach((node) => {
    pos.x[x] = pos.x[x] == null ? 1 : pos.x[x] + 25;
    pos.y[y] = pos.y[y] == null ? 1 : pos.y[y] + 25;

    x += (node[1].length + 1) * 15 + pos.x[x];
    y += (node[1].length + 1) *2 + pos.y[y];
    nodeDataArray.push({
      key: node[0],
      fields: node[1],
      loc: x + " " + y,
    });
  });

  return { nodeDataArray: nodeDataArray, linkDataArray: linkDataArray };
}

function relToField(nodes, table, value, comment) {
  exists = false;
  if (nodes[table] == null) {
    let field = {
      name: value,
      info: comment,
      color: "#ffffff",
      figure: "Ellipse",
    };
    nodes[table] = [field];
  } else {
    for (let i in nodes[table]) {
      if (nodes[table][i].name == value) {
        nodes[table][i].info =
          nodes[table][i].info != ""
            ? nodes[table][i].info[0] == "#"
              ? nodes[table][i].info + comment
              : ""
            : "";
        exists = true;
      }
    }
    if (!exists) {
      let field = {
        name: value,
        info: comment,
        color: "#ffffff",
        figure: "Ellipse",
      };
      nodes[table].push(field);
    }
  }
  return nodes;
}

function mermaidSubgraph(table_name, value_name, comment) {
  return "\nsubgraph " + table_name + "\n" + value_name + comment + "\nend ";
}

function jsonToMermaid(obj) {
  let output = "";
  obj.forEach((element) => {
    if (element.target_val == undefined) {
      console.log(element);
    }
    let targetval =
      element.target_val == false ? element.origin_val : element.target_val;
    let leftRelation =
      element.origin_table != ""
        ? element.origin_table + "." + element.origin_val
        : element.origin_val;
    let rightRelation =
      element.target_table != ""
        ? element.target_table + "." + targetval
        : targetval;
    if (leftRelation != rightRelation) {
      if (element.origin_val == "*") {
        output += mermaidSubgraph(
          element.origin_table,
          mermaidSubgraph(element.target_table, rightRelation, "(*)"),
          ""
        );
      } else {
        output +=
          "\n" +
          leftRelation +
          " --" +
          element.comment +
          "--> " +
          rightRelation;
      }
    }
    if (element.target_table != "") {
      output += mermaidSubgraph(
        element.target_table,
        element.target_table + "." + targetval,
        "(" + targetval + ")"
      );
    }
    if (element.origin_table != "" && element.origin_val != "*") {
      output += mermaidSubgraph(
        element.origin_table,
        element.origin_table + "." + element.origin_val,
        "(" + element.origin_val + ")"
      );
    }
  });
  return output != "" ? "graph TB" + output : output;
}

module.exports = router;
