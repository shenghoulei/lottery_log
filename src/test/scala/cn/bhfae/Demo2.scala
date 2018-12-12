package cn.bhfae

import org.junit.Test


class Demo2 {
	@Test
	def test: Unit = {
		val str = "盛世集团"
		val bytes = str.getBytes("GBK")
		val str2 = new String(bytes, "GBK")
		println(str2)
	}

}
