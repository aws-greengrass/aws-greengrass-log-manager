Feature: Testing Cloud component in Greengrass

  Background:
    Given my device is registered as a Thing
    And my device is running Greengrass

  @HelloWorldTst @CloudDeployment @stable
  Scenario: As a developer, I can create a component in Cloud and deploy it on my device
    When I create a Greengrass deployment with components
      | com.aws.HelloWorld | classpath:/greengrass/recipes/recipe.yaml |
    # Deployment with new version
    When I create a Greengrass deployment with components
      | com.aws.HelloWorld | classpath:/greengrass/recipes/updated_recipe.yaml |



