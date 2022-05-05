package dit.anonymous.webapp.models;

import org.springframework.data.repository.CrudRepository;

public interface WorkflowConfigurationRepository extends CrudRepository<WorkflowConfiguration, Integer>{
	
	WorkflowConfiguration findById(int id);
	
	boolean existsById(int id);

}
