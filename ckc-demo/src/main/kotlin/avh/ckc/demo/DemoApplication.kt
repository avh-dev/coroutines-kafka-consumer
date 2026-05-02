package avh.ckc.demo

import avh.ckc.demo.config.DemoApplicationProperties
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@SpringBootApplication
@EnableConfigurationProperties(DemoApplicationProperties::class)
open class DemoApplication

fun main(args: Array<String>) {
    runApplication<DemoApplication>(*args)
}
