var stats = {
    'relations' : {},
    'ways' : {},
    'all_docs': {},
    'fltrd_docs' : {}
};

stats['relations']['max_members'] = db.pvd.aggregate( [ {$match :{"datatype":"relation"} }, {$group : { _id : null, count : { $max : {$size : '$member' } } } } ] )._batch;
stats['relations']['avg_members'] =db.pvd.aggregate( [ {$match :{"datatype":"relation"} }, {$group : { _id : null, count : { $avg : {$size : '$member' } } } } ] )._batch;
stats['relations']['min_members'] =db.pvd.aggregate( [ {$match :{"datatype":"relation"} }, {$group : { _id : null, count : { $min : {$size : '$member' } } } } ] )._batch;

stats['ways']['avg_nd'] = db.pvd.aggregate( [ {$match :{"datatype":"way"} }, {$group : { _id : null, count : { $avg : {$size : '$nd' } } } } ] )._batch;
stats['ways']['min_nd'] = db.pvd.aggregate( [ {$match :{"datatype":"way"} }, {$group : { _id : null, count : { $min : {$size : '$nd' } } } } ] )._batch;
stats['ways']['max_nd'] = db.pvd.aggregate( [ {$match :{"datatype":"way"} }, {$group : { _id : null, count : { $max : {$size : '$nd' } } } } ] )._batch;

// All documents
stats['all_docs']['field_count_avg'] = db.pvd.aggregate( [ { '$project' : { 'datatype': '$datatype', 'field_count' : { $size : { $filter : { input: { $objectToArray : "$$CURRENT" }, as:'field', cond: {$not : { $in : ['$$field.k', ['_id','created','datatype'] ] } } } } } } }, {$group : {'_id' : '$datatype', 'fields' : { $avg : '$field_count' } } } ] )._batch;
stats['all_docs']['field_count_std'] = db.pvd.aggregate( [ { '$project' : { 'datatype': '$datatype', 'field_count' : { $size : { $filter : { input: { $objectToArray : "$$CURRENT" }, as:'field', cond: {$not : { $in : ['$$field.k', ['_id','created','datatype'] ] } } } } } } }, {$group : {'_id' : '$datatype', 'fields' : { $stdDevPop : '$field_count' } } } ] )._batch;
stats['all_docs']['field_count_max'] = db.pvd.aggregate( [ { '$project' : { 'datatype': '$datatype', 'field_count' : { $size : { $filter : { input: { $objectToArray : "$$CURRENT" }, as:'field', cond: {$not : { $in : ['$$field.k', ['_id','created','datatype'] ] } } } } } } }, {$group : {'_id' : '$datatype', 'fields' : { $max : '$field_count' } } } ] )._batch;
stats['all_docs']['field_count_min'] = db.pvd.aggregate( [ { '$project' : { 'datatype': '$datatype', 'field_count' : { $size : { $filter : { input: { $objectToArray : "$$CURRENT" }, as:'field', cond: {$not : { $in : ['$$field.k', ['_id','created','datatype'] ] } } } } } } }, {$group : {'_id' : '$datatype', 'fields' : { $min : '$field_count' } } } ] )._batch;

// Filtered documents, with more than base attributes 
stats['fltrd_docs']['docs_with_1_field'] = db.pvd.aggregate( [ {$project : { 'datatype': '$datatype', 'field_count' : { $size : { $filter : { input: { $objectToArray : "$$CURRENT" }, as:'field', cond: {$not : { $in : ['$$field.k', ['_id','created','datatype'] ] } } } } } } }, {$match : {'field_count': 1} }, {$group : { '_id' : '$datatype', 'count' : { $sum :1 } } }  ]  )._batch;
stats['fltrd_docs']['docs_with_gt_1_field_count'] = db.pvd.aggregate( [ {$project : { 'datatype': '$datatype', 'field_count' : { $size : { $filter : { input: { $objectToArray : "$$CURRENT" }, as:'field', cond: {$not : { $in : ['$$field.k', ['_id','created','datatype'] ] } } } } } } }, {$match : {'field_count': { $gt : 1 } } }, {$group : { '_id' : '$datatype', 'count' : { $sum :1 } } }  ]  )._batch;
stats['fltrd_docs']['docs_with_gt_1_field_avg'] = db.pvd.aggregate( [ {$project : { 'datatype': '$datatype', 'field_count' : { $size : { $filter : { input: { $objectToArray : "$$CURRENT" }, as:'field', cond: {$not : { $in : ['$$field.k', ['_id','created','datatype'] ] } } } } } } }, {$match : {'field_count': { $gt : 1 } } }, {$group : { '_id' : '$datatype', 'count' : { $avg : '$field_count' } } }  ]  )._batch;
stats['fltrd_docs']['docs_with_gt_1_field_std'] = db.pvd.aggregate( [ {$project : { 'datatype': '$datatype', 'field_count' : { $size : { $filter : { input: { $objectToArray : "$$CURRENT" }, as:'field', cond: {$not : { $in : ['$$field.k', ['_id','created','datatype'] ] } } } } } } }, {$match : {'field_count': { $gt : 1 } } }, {$group : { '_id' : '$datatype', 'count' : { $stdDevPop : '$field_count' } } }  ]  )._batch;
printjson(stats)