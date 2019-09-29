package kazusato.eswork.updatebyquery

import kazusato.eswork.updatebyquery.command.NoopCommand
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class UpdateByQueryWorkTest {

    @Test
    fun testProcessArgsOneArg() {
        val ubq = UpdateByQueryWork(arrayOf("list"))
        val separated = ubq.separateArgs()
        val command = separated.first
        val cmdArgs = separated.second
        assertThat(command).isEqualTo("list")
        assertThat(cmdArgs).isEqualTo(arrayOf<String>())
    }

    @Test
    fun testProcessArgsTwoArgs() {
        val ubq = UpdateByQueryWork(arrayOf("list", "aaa"))
        val separated = ubq.separateArgs()
        val command = separated.first
        val cmdArgs = separated.second
        assertThat(command).isEqualTo("list")
        assertThat(cmdArgs).isEqualTo(arrayOf<String>("aaa"))
    }

    @Test
    fun testProcessArgsMultipleArgs() {
        val ubq = UpdateByQueryWork(arrayOf("list", "aaa", "bbb", "ccc"))
        val separated = ubq.separateArgs()
        val command = separated.first
        val cmdArgs = separated.second
        assertThat(command).isEqualTo("list")
        assertThat(cmdArgs).isEqualTo(arrayOf<String>("aaa", "bbb", "ccc"))
    }

    @Test
    fun testDispatchCommandNoop() {
        val ubq = UpdateByQueryWork(arrayOf("noop"))
        val separated = ubq.separateArgs()
        val command = ubq.dispatchCommand(separated.first, separated.second)
        assertThat(command).isInstanceOf(NoopCommand::class.java)
    }

}
