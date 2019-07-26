import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import java.util.{Calendar, Locale}
import Array._


object multiInheritanceEval {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]");
    val spark = SparkSession.builder.config(sparkConf).getOrCreate();
    val sc = spark.sparkContext;

    sc.setLogLevel("ERROR");

    val data_name = "depth7_branches_up_to5max_inheritance_4"



    val data_folder = "/Users/xu/Documents/Doctorat/git_workplace/lascar19/multi_inheritance_eval/";
    val data_path = data_folder + data_name + "/";
    val ontology_file = "TBOX_" + data_name + ".nt";
    val facts_file = "ABOX_" + data_name + ".nt";

    val ontology = sc.textFile(data_path + ontology_file).map(line => line.split(" ")).map(x => (x(2),x(0)));
    //println(ontology.count())
    //ontology.foreach(println)

    val concepts = ontology.flatMap(x => Array(x._1,x._2)).distinct();
    //concepts.foreach(println)
    //println(concepts.count())



    //////////////////
    ///// Start LiteMat encoding
    /////////////////

    val start_LiteMat = System.currentTimeMillis();

    val in_deg = ontology.map(x => (x._2,1)).reduceByKey(_+_);
    // Find all the nodes with multi-inheritance
    val multi_inheritance_nodes = in_deg.filter(x => x._2 > 1);

    def reconstructTriples(in: (String, Array[String])): List[(String, String)] ={
      var list_triples: List[(String, String)] = Nil;
      for(i <- in._2){
        list_triples = list_triples :+ (in._1, i);
      }
      return list_triples;
    }

    //Find all the triples with multi_inheritance_nodes as objects
    val pre_non_rep_triples = multi_inheritance_nodes.join(ontology.map(x => (x._2, x._1))).map(x => (x._1, x._2._2));
    //println(pre_non_rep_triples.count())

    //Select the presentative among ancestors and get the non_rep triples (delete triples with represnetative from pre_non_rep_triples )
    val non_rep_triples = pre_non_rep_triples.map(x => (x._1, Array(x._2))).reduceByKey(concat(_,_)).map(x => (x._1, x._2.slice(1, x._2.length))).flatMap(x => reconstructTriples(x)).map(x => (x._2, x._1));
    //pre_non_rep_triples.map(x => (x._2, x._1)).subtract(non_rep_triples).foreach(println)
    //println(non_rep_triples.count())

    // nonRep(x) = {...} in form of (x, Array(...))
    val non_rep = non_rep_triples.map(x => (x._1, Array(x._2))).reduceByKey(concat(_,_));
    //non_rep.foreach(println)

    val ontology_encoding_triples = ontology.subtract(non_rep_triples);
    //println(ontology_encoding_triples.count())


    /*
    var dict_tbox: List[(String, Long, Int)] = Nil;
    var encoding_concepts = concepts.subtract(ontology_encoding_triples.map(x => x._2).distinct());
    //encoding_concepts.foreach(println)
    var local_bit_length = Math.ceil(Math.log(encoding_concepts.count() + 1) / Math.log(2)).toInt;
    var encoding_concepts_local = encoding_concepts.collect();
    var dict_temp: List[(String, Long, Int)] = Nil;
    var id_local:Long = 1;
    for(i <- encoding_concepts_local){
      dict_temp = dict_temp :+ (i, id_local, local_bit_length);
      id_local += 1;
    }
    dict_tbox = dict_tbox:::dict_temp;
    var last_encoding_concepts = encoding_concepts; //rdd
    var rest_encoding_triples = ontology_encoding_triples.subtract(last_encoding_concepts.map(x => (x, 0)).join(ontology_encoding_triples.map(x => (x._2, x._1))).map(x => (x._2._2, x._1)));
    while (rest_encoding_triples.count() > 0){
      for(i <- last_encoding_concepts.collect()){
        rest_encoding_triples.filter(x => x._1 == i).
      }

    }*/


    // Distribute local id to each concept in form of (SuperClassName, (ClassName, local_id, local_length))
    def localIdentifier(node_relations: (String, Iterable[String])): List[(String,(String, Long, Int))] = {
      val local_length = Math.ceil(Math.log(node_relations._2.toArray.length + 1) / Math.log(2)).toInt;
      var local_id: Long = 1;
      var list_dict: List[(String,(String, Long, Int))] = Nil;
      for(i <- node_relations._2){
        list_dict = list_dict :+ (node_relations._1, (i, local_id, local_length));
        local_id += 1;
      }
      return list_dict;
    }

    val dict_local_id = ontology_encoding_triples.groupByKey().flatMap(x => localIdentifier(x));

    //Calculate the root concepts
    val top_concepts = concepts.subtract(ontology_encoding_triples.map(x => x._2));
    val top_length = Math.ceil(Math.log(top_concepts.count() + 1) / Math.log(2)).toInt;
    var top_dict: List[(String, Long, Int)] = Nil;
    var top_id: Long = 1;
    for(i <- top_concepts.collect()){
      top_dict = top_dict :+ (i, top_id, top_length);
      top_id += 1;
    }
    var temp_dict = sc.parallelize(top_dict);
    val concepts_number = concepts.count();
    var last_step_dict = temp_dict;

    // Calculate encodings of all the concepts
    while(temp_dict.count() < concepts_number){
      last_step_dict = last_step_dict.map(x => (x._1, (x._2, x._3))).join(dict_local_id).map(x => (x._2._2._1, (x._2._1._1 << x._2._2._3) + x._2._2._2, x._2._1._2 + x._2._2._3));
      temp_dict = temp_dict.union(last_step_dict);
    }

    val encoding_length = temp_dict.map(x => x._3).reduce(Math.max(_,_));
    val concepts_dict_LiteMat = temp_dict.map(x => (x._1, (x._2 << encoding_length - x._3), x._3));

    // nonRep(x) = {x1,x2...} in form of (id_x, List((id_x1, encoding_length_x1),(id_x2, encoding_length_x2),...))
    val non_rep_encoding = non_rep_triples.map(x => (x._2, x._1)).join(concepts_dict_LiteMat.map(x => (x._1, (x._2, x._3)))).map(x => (x._2._1, List(x._2._2))).reduceByKey(_:::_).join(concepts_dict_LiteMat.map(x => (x._1, x._2))).map(x => (x._2._2, x._2._1));
    //non_rep_encoding.foreach(println)


    val initialize_time_LiteMat = System.currentTimeMillis() - start_LiteMat;
    println("************************************************************")
    println("*  LiteMat initialization time: " + initialize_time_LiteMat)
    println("************************************************************")


    /// Save as file
    concepts_dict_LiteMat.map(x => x._1 + " " + x._2 + " " + x._3).repartition(1).saveAsTextFile(data_path + "LiteMat/dict")
    non_rep_encoding.map(x => x._1.toString + " " + {
      var temp_list: String = ""
      for(i <- x._2){
        temp_list = temp_list + i._1.toString + " " + i._2.toString + " ";
      }
      temp_list
    }).repartition(1).saveAsTextFile(data_path + "LiteMat/nonRep")



    //////////////////////////////
    //// FullMat section
    //////////////////////////////



    val start_FullMat = System.currentTimeMillis();


    val dict_full_mat = concepts.zipWithUniqueId();
    //dict_full_mat.foreach(println)

    // replace all the concepts with ids
    val full_mat_ontology = ontology.join(dict_full_mat).map(x => (x._2._1, x._2._2)).join(dict_full_mat).map(x => (x._2._1, x._2._2));
    //full_mat_triples.foreach(println)

    // Start_concepts are the concepts with no sub-concepts
    val start_concepts = dict_full_mat.map(x => x._2).subtract(full_mat_ontology.map{case (super_concept, sub_concept) => super_concept});
    var last_treat_concepts = start_concepts.map(x => (x, 0)).join(full_mat_ontology.map(x => (x._2, x._1))).map(x => x._2._2);
    var new_generate_triples = start_concepts.map(x => (x, 0)).join(full_mat_ontology.map(x => (x._2, x._1))).map(x => (x._2._2, x._1));
    var triple_number: Long = 0;

    // Each step, generate new triples by using (the triples in which last_treat_concepts appear as subConcept)
    // and (newly generated triples of last step), until no more triples newly generated
    while(new_generate_triples.count() != triple_number){
      triple_number = new_generate_triples.count();
      val new_considered_triples = last_treat_concepts.map(x => (x, 0)).join(full_mat_ontology.map(x => (x._2, x._1))).map(x => (x._2._2, x._1));
      new_generate_triples = new_generate_triples.union(new_generate_triples.join(new_considered_triples.map(x => (x._2, x._1))).map(x => (x._2._2, x._2._1))).union(new_considered_triples).distinct();
      last_treat_concepts = new_considered_triples.map(x => x._1);
    }

    val full_mat_triples = new_generate_triples;
    //println(full_mat_triples.count())
    //full_mat_triples.join(dict_full_mat.map(x => (x._2, x._1))).map(x => x._2).join(dict_full_mat.map(x => (x._2, x._1))).map(x => x._2).groupByKey().foreach(println)

    val initialize_time_FullMat = System.currentTimeMillis() - start_FullMat;

    println("************************************************************")
    println("*  FullMat initialization time: " + initialize_time_FullMat)
    println("************************************************************")


    // Save file

    dict_full_mat.map(x => x._1 + " " + x._2).repartition(1).saveAsTextFile(data_path + "FullMat/dict")
    full_mat_triples.map(x => x._1 + " 1 " + x._2).repartition(1).saveAsTextFile(data_path + "FullMat/tripleStore")

    ////////////////////
    /// Query test
    ////////////////////

    import util.Random.nextInt

    // Generate 15 queries in form of {?x rdf:type Concept} where Concept is a constant randomly chosen
    val query_concepts = {
      var list_concepts: List[String] = Nil;
      val number_concepts = concepts.count();
      for(i <- Range(0,15)){
        list_concepts = list_concepts :+ "<http://www.multiinhertance.com/concept" + nextInt(Math.ceil(number_concepts/2).toInt) + ">"
      }
      list_concepts
    };

    val facts = sc.textFile(data_path + facts_file).map(line => line.split(" ")).map(x => (x(0),x(2)));
    facts.persist();

    ///////////////////
    //// LiteMat Query
    ///////////////////

    concepts_dict_LiteMat.persist();
    non_rep_encoding.persist();

    var query_time_list_LiteMat: List[Long] = Nil;

    println("*************************************")
    println("LiteMat query test")
    println("*************************************")
    println("")
    println("Query pattern: {?x rdf:type Concept}")
    println("")


    for(i <- query_concepts){
      val start_query_time = System.currentTimeMillis();

      for(k <- Range(0, 5)){
        val concept_encoding = (concepts_dict_LiteMat.filter(x => x._1 == i).collect())(0)._2;
        val concept_encoding_length = (concepts_dict_LiteMat.filter(x => x._1 == i).collect())(0)._3;
        val lower_bound = concept_encoding;
        val upper_bound = concept_encoding + (1 << (encoding_length - concept_encoding_length));
        var sub_concepts_encoding = concepts_dict_LiteMat.filter(x => (x._2 >= lower_bound && x._2 < upper_bound)).map(x => x._2);
        var last_number: Long = 0;
        var extend_concepts = non_rep_encoding.filter(x => (x._1 >= lower_bound && x._1 < upper_bound)).flatMap(x => x._2).filter(x => (x._1 >= upper_bound || x._1 < lower_bound)).collect().toList;
        while(extend_concepts.length > 0 && last_number != sub_concepts_encoding.count()){
          last_number = sub_concepts_encoding.count();
          var new_extend_concepts: List[(Long, Int)] = Nil;
          for(i <- extend_concepts){
            val temp_lower_bound = i._1;
            val temp_upper_bound = i._1 + (1 << (encoding_length - i._2));
            sub_concepts_encoding = sub_concepts_encoding.union(concepts_dict_LiteMat.filter(x => (x._2 >= temp_lower_bound && x._2 < temp_upper_bound)).map(x => x._2));
            new_extend_concepts = new_extend_concepts ::: (non_rep_encoding.filter(x => (x._1 >= temp_lower_bound && x._1 < temp_upper_bound)).flatMap(x => x._2).filter(x => (x._1 >= temp_upper_bound || x._1 < temp_lower_bound)).collect().toList);
          }
          sub_concepts_encoding = sub_concepts_encoding.distinct();
          extend_concepts = new_extend_concepts;
        }

        val result_list_instance = facts.map(x => (x._2, x._1)).join(concepts_dict_LiteMat.map(x => (x._1, x._2))).map(x => (x._2._2, x._2._1)).join(sub_concepts_encoding.map(x => (x, 0))).map(x => x._2._1).distinct();

      }

      val query_time = (System.currentTimeMillis() - start_query_time) / 5;

      query_time_list_LiteMat = query_time_list_LiteMat :+ query_time.toLong;


      val concept_encoding = (concepts_dict_LiteMat.filter(x => x._1 == i).collect())(0)._2;
      val concept_encoding_length = (concepts_dict_LiteMat.filter(x => x._1 == i).collect())(0)._3;
      val lower_bound = concept_encoding;
      val upper_bound = concept_encoding + (1 << (encoding_length - concept_encoding_length));
      var sub_concepts_encoding = concepts_dict_LiteMat.filter(x => (x._2 >= lower_bound && x._2 < upper_bound)).map(x => x._2);
      var last_number: Long = 0;
      var extend_concepts = non_rep_encoding.filter(x => (x._1 >= lower_bound && x._1 < upper_bound)).flatMap(x => x._2).filter(x => (x._1 >= upper_bound || x._1 < lower_bound)).collect().toList;
      while(extend_concepts.length > 0 && last_number != sub_concepts_encoding.count()){
        last_number = sub_concepts_encoding.count();
        var new_extend_concepts: List[(Long, Int)] = Nil;
        for(i <- extend_concepts){
          val temp_lower_bound = i._1;
          val temp_upper_bound = i._1 + (1 << (encoding_length - i._2));
          sub_concepts_encoding = sub_concepts_encoding.union(concepts_dict_LiteMat.filter(x => (x._2 >= temp_lower_bound && x._2 < temp_upper_bound)).map(x => x._2));
          new_extend_concepts = new_extend_concepts ::: (non_rep_encoding.filter(x => (x._1 >= temp_lower_bound && x._1 < temp_upper_bound)).flatMap(x => x._2).filter(x => (x._1 >= temp_upper_bound || x._1 < temp_lower_bound)).collect().toList);
        }
        sub_concepts_encoding = sub_concepts_encoding.distinct();
        extend_concepts = new_extend_concepts;
      }

      val result_list_instances = facts.map(x => (x._2, x._1)).join(concepts_dict_LiteMat.map(x => (x._1, x._2))).map(x => (x._2._2, x._2._1)).join(sub_concepts_encoding.map(x => (x, 0))).map(x => x._2._1).collect();

      println("Concept: " + i +" has " + result_list_instances.length + " matched instances. Query time: " + query_time + " ms.")

    }

    println({
      var out: String = "";
      for(i <- query_time_list_LiteMat){
        out += i.toString + " ";
      }
      out
    })


    concepts_dict_LiteMat.unpersist();
    non_rep_encoding.unpersist();

    ///////////////////
    //// FullMat Query
    ///////////////////

    dict_full_mat.persist();
    full_mat_triples.persist();

    var query_time_list: List[Long] = Nil;

    println("")
    println("")
    println("")
    println("*************************************")
    println("FullMat query test")
    println("*************************************")
    println("")
    println("Query pattern: {?x rdf:type Concept}")
    println("")


    for(i <- query_concepts){
      val start_query_time = System.currentTimeMillis();

      // execute each query 5 times and get the average
      for(k <- Range(0,5)){
        val id_i = (dict_full_mat.filter(x => x._1 == i).collect()).head._2;
        val sub_concepts_id = full_mat_triples.filter( x => x._1 == id_i).map(x => x._2);
        val sub_concepts = sub_concepts_id.map(x => (x, 0)).join(dict_full_mat.map(x => (x._2, x._1))).map(x => x._2._2);
        val result_list_instances =  sub_concepts.map(x => (x, 0)).join(facts.map(x => (x._2, x._1))).map(x => x._2._2).collect();
      }

      val query_time = (System.currentTimeMillis() - start_query_time) / 5;

      query_time_list = query_time_list :+ query_time.toLong;

      val id_i = (dict_full_mat.filter(x => x._1 == i).collect()).head._2;
      val sub_concepts_id = full_mat_triples.filter( x => x._1 == id_i).map(x => x._2).union(dict_full_mat.filter(x => x._1 == i).map(x => x._2));
      val sub_concepts = sub_concepts_id.map(x => (x, 0)).join(dict_full_mat.map(x => (x._2, x._1))).map(x => x._2._2);
      val result_list_instances =  sub_concepts.map(x => (x, 0)).join(facts.map(x => (x._2, x._1))).map(x => x._2._2).collect();

      println("Concept: " + i +" has " + result_list_instances.length + " matched instances. Query time: " + query_time + " ms.")
    }

    println({
      var out: String = "";
      for(i <- query_time_list){
        out += i.toString + " ";
      }
      out
    })
    dict_full_mat.unpersist();
    full_mat_triples.unpersist();


    facts.unpersist();
  }
}
