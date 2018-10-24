package com.example.demo;

import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;



@RestController
public class DemoController {
	@Autowired
	private EmployeeRepository employeeRepository;

	@RequestMapping(value = "/welcome",method=RequestMethod.POST)
	public String myMethod(@RequestBody Employee emp) {
		
		System.out.println("hi");
		/*Employee emp=new Employee();
		emp.setEid(3);
		emp.setFirst_name("Niramala");
		emp.setLast_name("Nayana");
		emp.setDepartment("CSE");*/
		
		employeeRepository.save(emp);
		
		return "data is succefully stored into database";
	}
	@RequestMapping("/fetchalldata")
	public List<Employee> getEmployee(){
		List<Employee> list = employeeRepository.findAll();
		return list;
		
	}
	
	@PostConstruct
	public void myMethod() {
		MqttKafkaBridge mqttKafkaBridge = new MqttKafkaBridge();

		mqttKafkaBridge.start();

		//return "hi";
		System.out.println("hi");
		
	}
}
