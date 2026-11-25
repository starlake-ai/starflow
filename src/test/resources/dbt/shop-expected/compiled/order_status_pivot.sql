select
    customer_id,
    
  
    sum(
      
      case
      when status = 'completed'
        then 1
      else 0
      end
    )
    
      
            as "completed"
      
    
    ,
  
    sum(
      
      case
      when status = 'placed'
        then 1
      else 0
      end
    )
    
      
            as "placed"
      
    
    ,
  
    sum(
      
      case
      when status = 'returned'
        then 1
      else 0
      end
    )
    
      
            as "returned"
      
    
    
  

from "shop"."staging"."stg_orders"
group by customer_id