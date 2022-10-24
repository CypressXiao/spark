package BigCase.beans

/**
 * @projectName: spark 
 * @package: BigCase.beans
 * @caseClassName: UserVisitAction
 * @author: Cypress_Xiao
 * @description: 用户行为bean样例类
 * @date: 2022/7/2 21:45
 * @version: 1.0
 */
case class UserVisitAction(
                            date:String, //用户点击行为的日期
                            user_id:Long, //用户的 ID
                            session_id:String, //Session 的 ID
                            page_id:Long, //某个页面的 ID
                            action_time:String, //动作的时间点
                            search_keyword:String, //用户搜索的关键词
                            click_category_id:Long, //某一个商品品类的 ID
                            click_product_id:Long, //某一个商品的 ID
                            order_category_ids:String, //一次订单中所有品类的 ID 集合
                            order_product_ids:String, //一次订单中所有商品的 ID 集合
                            pay_category_ids:String, //一次支付中所有品类的 ID 集合
                            pay_product_ids:String, //一次支付中所有商品的 ID 集合
                            city_id:Long//城市 id
                          )
