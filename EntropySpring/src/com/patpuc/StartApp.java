package com.patpuc;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
@RequestMapping("/start")
public class StartApp{
 
   @RequestMapping(method = RequestMethod.GET)
   public String printHello(ModelMap model) throws Exception {
      model.addAttribute("message", "Entropy Calculator is working now.");
      String[] arg = new String[2];
      arg[0]= "/user/pusiux/input";
      arg[0]= "/user/pusiux/output";
      EntropyBase.main(arg);   
      return "start";
   }

}

