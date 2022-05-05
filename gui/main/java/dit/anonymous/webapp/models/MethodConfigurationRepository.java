package dit.anonymous.webapp.models;

import org.springframework.data.repository.CrudRepository;


public interface MethodConfigurationRepository extends CrudRepository<MethodConfiguration, Integer>{
	
	MethodConfiguration findById(int id);

}
