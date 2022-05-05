package dit.anonymous.webapp.models;


import org.springframework.data.repository.CrudRepository;

public interface WorkflowResultsRepository extends CrudRepository<WorkflowResults, Integer>{

	WorkflowResults findById(int id);
	
	WorkflowResults findByworkflowID(int workflowID);
	
	boolean existsByworkflowID(int workflowID);

}
