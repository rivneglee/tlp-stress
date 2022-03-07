package com.thelastpickle.tlpstress.profiles

import com.datastax.driver.core.Session
import com.thelastpickle.tlpstress.PartitionKey
import com.thelastpickle.tlpstress.StressContext
import com.thelastpickle.tlpstress.WorkloadParameter
import kotlin.random.Random


class WideTable : IStressProfile {
    @WorkloadParameter(description = "The number of table columns")
    var columns = 300

    @WorkloadParameter(description = "The max count of features per entity")
    var maxFeaturesPerEntity = 30

    @WorkloadParameter(description = "The min count of features per entity")
    var minFeaturesPerEntity = 3

    @WorkloadParameter(description = "The max count of features per query")
    var maxFeaturesPerQuery = 20

    @WorkloadParameter(description = "The min count of features per query")
    var minFeaturesPerQuery = 3

    @WorkloadParameter(description = "The switch for insert null to empty columns")
    var insertNull = 0

    override fun schema(): List<String> {
        val columnList = StringBuilder()
        for (i in 0 until columns) {
            columnList.append(", feature_${i} text")
        }
        val query = """CREATE TABLE IF NOT EXISTS wide_table (
                            entity_id text PRIMARY KEY
                            $columnList
                            )
                           """.trimIndent()

        return listOf(query)
    }

    override fun prepare(session: Session) {}

    override fun getRunner(context: StressContext): IStressRunner {

        return object : IStressRunner {

            val generator = com.thelastpickle.tlpstress.generators.functions.Random().apply{min=100; max=200}

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val columnList = StringBuilder()
                val randomList = (0 until columns).shuffled().take(Random.nextInt(minFeaturesPerQuery, maxFeaturesPerQuery))
                randomList.forEach {
                    columnList.append(", feature_$it")
                }

                val statement = context.session.prepare("SELECT entity_id$columnList FROM wide_table WHERE entity_id = ?");
                val bound = statement.bind(partitionKey.getText())
                return Operation.SelectStatement(bound)
            }

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val columnList = StringBuilder()
                val valueList = StringBuilder()
                val nullColumns = StringBuilder()
                val nulls = StringBuilder()
                val randomList = (0 until columns).shuffled().take(Random.nextInt(minFeaturesPerEntity, maxFeaturesPerEntity))
                randomList.forEach {
                    columnList.append(", feature_$it")
                    valueList.append(", '${generator.getText()}'")
                }
                if (insertNull != 0) {
                    for (i in 0 until columns) {
                        if (!randomList.contains(i)) {
                            nullColumns.append(", feature_$i")
                            nulls.append(", null")
                        }
                    }
                }
                val statement = context.session.prepare("INSERT INTO wide_table (entity_id${columnList}${nullColumns}) VALUES (?${valueList}${nulls})");
                val bound = statement.bind(partitionKey.getText())

                return Operation.Mutation(bound)
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                val statement = context.session.prepare("DELETE from wide_table WHERE entity_id = ?")
                val bound = statement.bind(partitionKey.getText())
                return Operation.Deletion(bound)
            }
        }
    }
}